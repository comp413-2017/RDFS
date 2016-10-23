#include <iostream>
#include <asio.hpp>
#include <thread>

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
	asio::error_code error;
	uint16_t version;
	unsigned char type;
	uint64_t payload_size;
	if (receive_header(sock, &version, &type)) {
		LOG(INFO) << "Got header version=" << version << ", type=" << (int) type;
	} else {
		ERROR_AND_RETURN("Failed to receive header.");
	}
	// TODO: switch proto based on type
	OpReadBlockProto proto;
	if (rpcserver::read_proto(sock, proto)) {
		LOG(INFO) << "Op a read block proto";
		LOG(INFO) << proto.DebugString();
	} else {
		ERROR_AND_RETURN("Failed to op the read block proto.");
	}
	BlockOpResponseProto response;
	std::string responseString;
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
	response.SerializeToString(&responseString);
	LOG(INFO) << response.DebugString();
	LOG(INFO) << std::endl << responseString;
	if (rpcserver::write_delimited_proto(sock, responseString)) {
		LOG(INFO) << "Successfully sent response to client";
	} else {
		LOG(INFO) << "Could not send response to client";
	}
	// TODO: write the packet header. see PacketReceiver.java#doRead
	//rpcserver::write_int32(sock, 19);
	uint32_t i = 0;
	::google::protobuf::int64 offset = 0;
	int num_packets = 1;
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
		LOG(INFO) << p_head.DebugString();


		uint16_t header_len = p_head_str.length();
		uint32_t payload_len = 8;

		rpcserver::write_int32(sock, payload_len);
		rpcserver::write_int16(sock, header_len);
		if (rpcserver::write_proto(sock, p_head_str)) {
			LOG(INFO) << "Successfully sent packet header to client";
		} else {
			LOG(INFO) << "Could not send packet header to client";
		}
		rpcserver::write_int32(sock, i);
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

void TransferServer::serve(asio::io_service& io_service) {
	LOG(INFO) << "Transfer Server listens on :" << this->port;
	tcp::acceptor a(io_service, tcp::endpoint(tcp::v4(), this->port));

	for (;;) {
		tcp::socket sock(io_service);
		a.accept(sock);
		std::thread(&TransferServer::handle_connection, this, std::move(sock)).detach();
	}
}
