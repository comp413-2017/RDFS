#include <iostream>
#include <asio.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <thread>
#include <vector>
#include <functional>
#include <easylogging++.h>

#include "data_transfer_server.h"

#define ERROR_AND_RETURN(msg) LOG(ERROR) << msg; return

using asio::ip::tcp;
// the .proto file implementation's namespace, used for messages
using namespace hadoop::hdfs;

// Default from CommonConfigurationKeysPublic.java#IO_FILE_BUFFER_SIZE_DEFAULT
const size_t PACKET_PAYLOAD_BYTES = 4096 * 4;

TransferServer::TransferServer(int port, std::shared_ptr<nativefs::NativeFS> &fs, std::shared_ptr<zkclient::ZkClientDn> &dn, int max_xmits) : port(port), fs(fs), dn(dn), max_xmits(max_xmits) {}

bool TransferServer::receive_header(tcp::socket& sock, uint16_t* version, unsigned char* type) {
	return (rpcserver::read_int16(sock, version) && rpcserver::read_byte(sock, type));
}

bool TransferServer::write_header(tcp::socket& sock, uint16_t version, uint8_t type) {
	return (rpcserver::write_int16(sock, version) && rpcserver::write_byte(sock, type));
}

void TransferServer::handle_connection(tcp::socket sock) {
	for (;;) {
		asio::error_code error;
		uint16_t version;
		unsigned char type;
		if (receive_header(sock, &version, &type)) {
			LOG(INFO) << "Got header version=" << version << ", type=" << (int) type;
		} else {
			ERROR_AND_RETURN("Failed to receive header.");
		}
		// TODO: implement proto handlers based on type
		switch (type) {
			case (WRITE_BLOCK): {
				std::function <void(TransferServer&, tcp::socket&)> writeFn = &TransferServer::processWriteRequest;
				synchronize(writeFn, sock);
			}
			break;
			case (READ_BLOCK): {
				std::function <void(TransferServer&, tcp::socket&)> readFn = &TransferServer::processReadRequest;
				synchronize(readFn, sock);
			}
			break;
			case (READ_METADATA):
				ERROR_AND_RETURN("Handler for read-metadata not written yet.");
			case (REPLACE_BLOCK):
				ERROR_AND_RETURN("Handler for replace-block not written yet.");
			case (COPY_BLOCK):
				ERROR_AND_RETURN("Handler for copy-block not written yet.");
			case (BLOCK_CHECKSUM):
				ERROR_AND_RETURN("Handler for block-checksum not written yet.");
			case (TRANSFER_BLOCK):
				ERROR_AND_RETURN("Handler for transfer-block not written yet.");
			case (REQUEST_SHORT_CIRCUIT_FDS):
				ERROR_AND_RETURN("Handler for request-short-circuit-fds not written yet.");
			case (RELEASE_SHORT_CIRCUIT_FDS):
				ERROR_AND_RETURN("Handler for release-short-circuit-fds not written yet.");
			case (REQUEST_SHORT_CIRCUIT_SHM):
				ERROR_AND_RETURN("Handler for request-short-circuit-shm not written yet.");
			case (BLOCK_GROUP_CHECKSUM):
				ERROR_AND_RETURN("Handler for block-group-checksum not written yet.");
			case (CUSTOM):
				ERROR_AND_RETURN("Handler for custom-op not written yet.");
			default:
				//Error
				ERROR_AND_RETURN("Unknown operation type specified.");
		}
	}
}

void TransferServer::processWriteRequest(tcp::socket& sock) {
	OpWriteBlockProto proto;
	if (rpcserver::read_delimited_proto(sock, proto)) {
		LOG(INFO) << "Op a write block proto";
		LOG(INFO) << proto.DebugString();
	} else {
		ERROR_AND_RETURN("Failed to op the write block proto.");
	}
	std::string response_string;
	buildBlockOpResponse(response_string);

	const ClientOperationHeaderProto header = proto.header();
	std::vector<DatanodeInfoProto> targets;
	for (int i = 0; i < proto.targets_size(); i++) {
		targets.push_back(proto.targets(i));
	}

	const DatanodeInfoProto src = proto.source();
	const OpWriteBlockProto_BlockConstructionStage stage = proto.stage();
	int pipelineSize = proto.pipelinesize();
	//num bytes in block
	uint64_t bytesInBlock = proto.minbytesrcvd();
	//num bytes sent
	uint64_t bytesSent = proto.maxbytesrcvd();

	if (rpcserver::write_delimited_proto(sock, response_string)) {
		LOG(INFO) << "Successfully sent response to client";
	} else {
		ERROR_AND_RETURN("Could not send response to client");
	}

	// read packets of block from client
	bool last_packet = false;
	std::string block_data;
	int max_capacity = bytesInBlock / PACKET_PAYLOAD_BYTES + 1; //1 added to include the empty termination packet
	boost::lockfree::spsc_queue<PacketHeaderProto> ackQueue(max_capacity);
	auto ackThread = std::thread(&TransferServer::ackPackets, this, std::ref(sock), std::ref(ackQueue));
	while (!last_packet) {
		asio::error_code error;
		uint32_t payload_len;
		rpcserver::read_int32(sock, &payload_len);
		uint16_t header_len;
		rpcserver::read_int16(sock, &header_len);
		PacketHeaderProto p_head;
		rpcserver::read_proto(sock, p_head, header_len);
		// LOG(INFO) << "Receigin packet " << p_head.seqno();
		last_packet = p_head.lastpacketinblock();
		uint64_t data_len = p_head.datalen();
		uint32_t checksum_len = payload_len - sizeof(uint32_t) - data_len;
		std::string checksum (checksum_len, 0);
		std::string data (data_len, 0);
		error = rpcserver::read_full(sock, asio::buffer(&checksum[0], checksum_len));
		if (error) {
			LOG(ERROR) << "Failed to read checksum for packet " << p_head.seqno();
			break;
		}
		error = rpcserver::read_full(sock, asio::buffer(&data[0], data_len));
		if (error) {
			LOG(ERROR) << "Failed to read packet " << p_head.seqno();
			break;
		}
		if (!last_packet && data_len != 0) {
			block_data += data;
		}
		// Wait free queue will return false on failure to insert element. Keep trying until insert works
		while (!ackQueue.push(p_head)) {
		}
	}

	if (!fs->writeBlock(header.baseheader().block().blockid(), block_data)) {
		LOG(ERROR) << "Failed to allocate block " << header.baseheader().block().blockid();
	} else {
		dn->blockReceived(header.baseheader().block().blockid(), block_data.length());
	}

	// TODO for the two datandoes in the list of datanodes to target that aren't me, pt the block
	// TODO I just wrote onto the replication queue belonging to those two datanodes!

	for (int i = 0; i < proto.targets_size(); i++) {
		const DatanodeIDProto& dn_p = proto.targets(i).id();
		std::string dn_name = dn_p.ipaddr() + ":" + std::to_string(dn_p.ipcport());
		if (!dn->push_dn_on_repq(dn_name, header.baseheader().block().blockid())) {
		}
	}

	LOG(INFO) << "Wait for acks to finish. ";
	ackThread.join();
}

void TransferServer::ackPackets(tcp::socket& sock, boost::lockfree::spsc_queue<PacketHeaderProto>& ackQueue) {
	bool last_packet = false;
	PacketHeaderProto p_head;
	do {
		// wait for packets to ack
		if (!ackQueue.pop(&p_head)) {
			continue;
		}

		// ack packet
		last_packet = p_head.lastpacketinblock();
		PipelineAckProto ack;
		ack.set_seqno(p_head.seqno());
		// LOG(INFO) << "Acking " << p_head.seqno();
		ack.add_reply(SUCCESS);
		std::string ack_string;
		ack.SerializeToString(&ack_string);
		if (rpcserver::write_delimited_proto(sock, ack_string)) {
			// LOG(INFO) << "Successfully sent ack to client";
		} else {
			LOG(ERROR) << "Could not send ack to client";
		}
	} while (!last_packet);
}

void TransferServer::processReadRequest(tcp::socket& sock) {

	OpReadBlockProto proto;
	if (rpcserver::read_delimited_proto(sock, proto)) {
		LOG(INFO) << "Op a read block proto" << std::endl << proto.DebugString();
	} else {
		ERROR_AND_RETURN("Failed to op the read block proto.");
	}
	std::string response_string;

	buildBlockOpResponse(response_string);
	if (rpcserver::write_delimited_proto(sock, response_string)) {
		LOG(INFO) << "Successfully sent response to client";
	} else {
		ERROR_AND_RETURN("Could not send BlockOpResponseProto in read request.");
	}

	uint64_t blockID = proto.header().baseheader().block().blockid();
	std::string block;
	bool success = fs->getBlock(blockID, block);
	if (!success) {
		LOG(ERROR) << "Failure on fs.getBlock";
	}

	uint64_t offset = proto.offset();
	uint64_t len = std::min(block.size() - offset, proto.len());
	if (offset > block.size()) {
		len = 0;
	}

	uint64_t seq = 0;
	while (len > 0) {
		PacketHeaderProto p_head;
		p_head.set_offsetinblock(offset);
		p_head.set_seqno(seq);
		p_head.set_lastpacketinblock(false);
		// The payload to write can be no more than what fits in the packet, or
		// remaining requested length.
		uint64_t payload_size = std::min(len, PACKET_PAYLOAD_BYTES);
		p_head.set_datalen(payload_size);
		p_head.set_syncblock(false);

		if (writePacket(sock, p_head, asio::buffer(&block[offset], payload_size))) {
			// LOG(INFO) << "Successfully sent packet " << seq << " to client";
			// LOG(INFO) << "Packet " << seq << " had " << payload_size << " bytes";
		} else {
			LOG(ERROR) << "Could not send packet " << seq << " to client";
			break;
		}
		offset += payload_size;
		// Decrement len, cannot underflow because payload size <= len.
		len -= payload_size;
		seq++;
	}

	if (len == 0) {
		// Finish by writing the last empty packet, if we finished.
		if (writeFinalPacket(sock, offset, seq)) {
			// Receive a status code from the client.
			ClientReadStatusProto status_proto;
			if (rpcserver::read_delimited_proto(sock, status_proto)) {
				LOG(INFO) << "Received read status from client.";
				LOG(INFO) << status_proto.DebugString();
			} else {
				LOG(ERROR) << "Could not read status from client.";
			}
		} else {
			LOG(ERROR) << "Could not send final packet to client";
		}
	}
}

// Write the final 0-payload packet to the client, and return whether
// successful.
bool TransferServer::writeFinalPacket(tcp::socket& sock, uint64_t offset, uint64_t seq) {
		PacketHeaderProto p_head;
		p_head.set_offsetinblock(offset);
		p_head.set_seqno(seq);
		p_head.set_lastpacketinblock(true);
		p_head.set_datalen(0);
		// No payload, so empty string.
		return writePacket(sock, p_head, asio::buffer(""));
}

void TransferServer::buildBlockOpResponse(std::string& response_string) {
	BlockOpResponseProto response;
	response.set_status(SUCCESS);
	OpBlockChecksumResponseProto* checksum_res = response.mutable_checksumresponse();
	checksum_res->set_bytespercrc(13);
	checksum_res->set_crcperblock(7);
	checksum_res->set_md5("this is my md5");
	ReadOpChecksumInfoProto* checksum_info = response.mutable_readopchecksuminfo();
	checksum_info->set_chunkoffset(0);
	ChecksumProto* checksum = checksum_info->mutable_checksum();
	checksum->set_type(CHECKSUM_NULL);
	checksum->set_bytesperchecksum(17);
	response.SerializeToString(&response_string);
	LOG(INFO) << std::endl << response.DebugString();
}

void TransferServer::serve(asio::io_service& io_service) {
	LOG(INFO) << "Transfer Server listens on :" << this->port;
	tcp::acceptor a(io_service, tcp::endpoint(tcp::v4(), this->port));

	for (;;) {
		tcp::socket sock(io_service);
		a.accept(sock);
		std::thread(&TransferServer::handle_connection, this, std::move(sock)).detach();
	}
}

void TransferServer::synchronize(std::function<void(TransferServer&, tcp::socket&)> f, tcp::socket& sock){
	std::unique_lock<std::mutex> lk(m);
	while (dn->getNumXmits() >= max_xmits){
		cv.wait(lk);
	}
	dn->incrementNumXmits();
	LOG(INFO) << "**********" << "num xmits is " << dn->getNumXmits();
	lk.unlock();
	f(*this, sock);
	dn->decrementNumXmits();
	cv.notify_one();
}

bool TransferServer::replicate(uint64_t len, std::string ip, std::string xferport, ExtendedBlockProto blockToTarget) {
	LOG(INFO) << " replicating length " << len << " with ip " << ip << " and port " << xferport;
	// connect to the datanode
	asio::io_service io_service;
	std::string port = xferport;
	tcp::resolver resolver(io_service);
	tcp::resolver::query query(ip, port);
	tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);

	tcp::socket sock(io_service);
	asio::connect(sock, endpoint_iterator);

	// construct the rpc op_read request
	std::string read_string;
	OpReadBlockProto read_block_proto;
	read_block_proto.set_len(len);
	ClientOperationHeaderProto* header = read_block_proto.mutable_header();
	header->set_clientname("datanode_replication"); // TODO ??

	BaseHeaderProto base_proto;
	base_proto.set_allocated_block(&blockToTarget);
	header->set_allocated_baseheader(&base_proto);

	// send the read request
	uint16_t version = 1; // TODO what is the version
	uint8_t read_request = READ_BLOCK;
	read_block_proto.SerializeToString(&read_string);
	if (!(write_header(sock, version, read_request) || rpcserver::write_proto(sock, read_string))) {
		LOG(ERROR) << " could not write the read request to target datanode";
		return false;
	}

	// read the read response
	BlockOpResponseProto read_response;
	if (!rpcserver::read_delimited_proto(sock, read_response)) {
		LOG(ERROR) << " could not read the read response from target datanode";
		return false;
	}
	std::string data(len, 0);

	// read in the packets while we still have them, and we haven't processed the final packet
	int read_len = 0;
	bool last_packet = false;
	while (read_len < len && !last_packet) {
		asio::error_code error;
		uint32_t payload_len;
		rpcserver::read_int32(sock, &payload_len);
		uint16_t header_len;
		rpcserver::read_int16(sock, &header_len);
		PacketHeaderProto p_head;
		rpcserver::read_proto(sock, p_head, header_len);
		LOG(INFO) << "Receigin packet " << p_head.seqno();
		// read in the data
		if (!p_head.lastpacketinblock()) {
			uint64_t data_len = p_head.datalen();
			uint64_t seqno = p_head.seqno();
			uint64_t offset = p_head.offsetinblock();
			error = rpcserver::read_full(sock, asio::buffer(&data[offset], data_len));
			if (error) {
				LOG(ERROR) << "Failed to read packet " << p_head.seqno();
				break;
			}
			read_len += data_len;
		} else {
			last_packet = true;
		}
	}

	if (!fs->writeBlock(header->baseheader().block().blockid(), data)) {
		LOG(ERROR) << "Failed to allocate block " << header->baseheader().block().blockid();
	} else {
		dn->blockReceived(header->baseheader().block().blockid(), read_len);
	}

	// close the connection
    sock.close();
    return true;
}
