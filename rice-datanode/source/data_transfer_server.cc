#include <iostream>
#include <asio.hpp>
#include <thread>
#include <vector>

#include <easylogging++.h>

#include "data_transfer_server.h"

#define ERROR_AND_RETURN(msg) LOG(ERROR) << msg; return

using asio::ip::tcp;
// the .proto file implementation's namespace, used for messages
using namespace hadoop::hdfs;

// Default from CommonConfigurationKeysPublic.java#IO_FILE_BUFFER_SIZE_DEFAULT
const size_t PACKET_PAYLOAD_BYTES = 4096;

TransferServer::TransferServer(int p, nativefs::NativeFS& fs, zkclient::ZkClientDn& dn) : port(p), fs(fs), dn(dn) {}

bool TransferServer::receive_header(tcp::socket& sock, uint16_t* version, unsigned char* type) {
	return (rpcserver::read_int16(sock, version) && rpcserver::read_byte(sock, type));
}

void TransferServer::handle_connection(tcp::socket sock) {
	for (;;) {
		asio::error_code error;
		uint16_t version;
		unsigned char type;
		uint64_t payload_size;
		if (receive_header(sock, &version, &type)) {
			LOG(INFO) << "Got header version=" << version << ", type=" << (int) type;
		} else {
			ERROR_AND_RETURN("Failed to receive header.");
		}
		// TODO: implement proto handlers based on type
		switch (type) {
			case (WRITE_BLOCK):
				processWriteRequest(sock);
				break;
			case (READ_BLOCK):
				processReadRequest(sock);
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
	std::queue<PacketHeaderProto> ackQueue;
	std::thread(&TransferServer::ackPackets, this, std::ref(sock), std::ref(ackQueue)).detach();
	while (!last_packet) {
		asio::error_code error;
		uint32_t payload_len;
		rpcserver::read_int32(sock, &payload_len);
		uint16_t header_len;
		rpcserver::read_int16(sock, &header_len);
		PacketHeaderProto p_head;
		rpcserver::read_proto(sock, p_head, header_len);
		last_packet = p_head.lastpacketinblock();
		uint64_t data_len = p_head.datalen();
		uint32_t checksum_len = payload_len - sizeof(uint32_t) - data_len;
		std::string checksum (checksum_len, 0);
		std::string data (data_len, 0);
		size_t len = sock.read_some(asio::buffer(&checksum[0], checksum_len), error);
		if (len != checksum_len || error) {
			LOG(ERROR) << "Failed to read checksum for packet " << p_head.seqno();
			break;
		}
		len = sock.read_some(asio::buffer(&data[0], data_len), error);
		if (len != data_len || error) {
			LOG(ERROR) << "Failed to read packet " << p_head.seqno();
			break;
		}
		if (!last_packet && data_len != 0) {
			block_data += data;
		}
		ackQueue.push(p_head);
	}

	LOG(INFO) << "Writing data to disk: " << block_data;
	if (!fs.writeBlock(header.baseheader().block().blockid(), block_data)) {
		LOG(ERROR) << "Failed to allocate block " << header.baseheader().block().blockid();
	} else {
		dn.blockReceived(header.baseheader().block().blockid(), bytesInBlock);
	}


	//TODO set proto source to this DataNode, remove this DataNode from
	//	the proto targets, and send this proto along to other
	//	DataNodes in targets
	//TODO read in a response (?)
	//TODO send packets to targets
}

void TransferServer::ackPackets(tcp::socket& sock, std::queue<PacketHeaderProto>& ackQueue) {
	bool last_packet = false;
	do {
		// wait for packets to ack
		if (ackQueue.empty()) {
			continue;
		}

		// ack packet
		PacketHeaderProto p_head = ackQueue.front();
		ackQueue.pop();
		last_packet = p_head.lastpacketinblock();
		PipelineAckProto ack;
		ack.set_seqno(p_head.seqno());
		ack.add_reply(SUCCESS);
		std::string ack_string;
		ack.SerializeToString(&ack_string);
		if (rpcserver::write_delimited_proto(sock, ack_string)) {
			LOG(INFO) << "Successfully sent ack to client";
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
	bool success;
	std::string block = this->fs.getBlock(blockID, success);
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
		std::string payload = block.substr(offset, payload_size);
		p_head.set_datalen(payload_size);

		if (writePacket(sock, p_head, asio::buffer(&payload[0], payload_size))) {
			LOG(INFO) << "Successfully sent packet " << seq << " to client";
			LOG(INFO) << "Packet " << seq << " had " << payload_size << " bytes";
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
