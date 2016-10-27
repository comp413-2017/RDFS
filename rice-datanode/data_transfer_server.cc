#include <iostream>
#include <asio.hpp>
#include <thread>
#include <vector>

#include <datatransfer.pb.h>

#include <easylogging++.h>

#include "socket_reads.h"
#include "socket_writes.h"
#include "rpcserver.h"
#include "data_transfer_server.h"

#define ERROR_AND_RETURN(msg) LOG(ERROR) << msg; return
#define ERROR_AND_FALSE(msg) LOG(ERROR) << msg; return false

using asio::ip::tcp;
// the .proto file implementation's namespace, used for messages
using namespace hadoop::hdfs;

TransferServer::TransferServer(int p) : port{p} {}

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

		sleep(5);
	}
}

void TransferServer::processWriteRequest(tcp::socket& sock) {
	// TODO: Consume 1 delimited OpWriteBlockProto
	OpWriteBlockProto proto;
	if (rpcserver::read_proto(sock, proto)) {
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
	::google::protobuf::uint64 bytesInBlock = proto.minbytesrcvd();
	//num bytes sent
	::google::protobuf::uint64 bytesSent = proto.maxbytesrcvd();

	if (rpcserver::write_delimited_proto(sock, response_string)) {
		LOG(INFO) << "Successfully sent response to client";
	} else {
		LOG(INFO) << "Could not send response to client";
	}

	//TODO read packets of block from client
	//TODO send acks
		

	//TODO write out to block
	

	//TODO set proto source to this DataNode, remove this DataNode from
	//	the proto targets, and send this proto along to other
	//	DataNodes in targets
	//TODO read in a response (?)
	//TODO send packets to targets
}

void TransferServer::processReadRequest(tcp::socket& sock) {
	OpReadBlockProto proto;
	if (rpcserver::read_proto(sock, proto)) {
		LOG(INFO) << "Op a read block proto";
		LOG(INFO) << proto.DebugString();
	} else {
		ERROR_AND_RETURN("Failed to op the read block proto.");
	}
	std::string response_string;
	buildBlockOpResponse(response_string);
	if (rpcserver::write_delimited_proto(sock, response_string)) {
		LOG(INFO) << "Successfully sent response to client";
	} else {
		LOG(INFO) << "Could not send response to client";
	}
	// TODO: write the packet header. see PacketReceiver.java#doRead
	//rpcserver::write_int32(sock, 19);
	uint32_t i = 0;
	::google::protobuf::int64 offset = 0;
	int num_packets = 1;
	char data[] = "abcd";
	while (i < num_packets + 1) {
		PacketHeaderProto p_head;
		p_head.set_offsetinblock(offset);
		p_head.set_seqno(i);
		if (i == num_packets) {
			p_head.set_lastpacketinblock(true);
			p_head.set_datalen(0);
		} else {
			p_head.set_lastpacketinblock(false);
			p_head.set_datalen(4);
		}
		std::string p_head_str;
		p_head.SerializeToString(&p_head_str);
		LOG(INFO) << std::endl << p_head_str;

		uint16_t header_len = p_head_str.length();
		uint32_t payload_len = 8;

		rpcserver::write_int32(sock, payload_len);
		rpcserver::write_int16(sock, header_len);
		if (rpcserver::write_proto(sock, p_head_str)) {
			LOG(INFO) << "Successfully sent packet header to client";
		} else {
			LOG(INFO) << "Could not send packet header to client";
		}
		sock.write_some(asio::buffer(data, 4));
		offset += 4;
		i++;
	}
	// Receive a status code from the client.
	ClientReadStatusProto status_proto;
	if (rpcserver::read_proto(sock, status_proto)) {
		LOG(INFO) << "Received read status from client.";
		LOG(INFO) << status_proto.DebugString();
	} else {
		LOG(INFO) << "Could not read status from client.";
	}
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
