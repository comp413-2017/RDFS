#include <iostream>
#include <asio.hpp>
#include <netinet/in.h>

#include <ProtobufRpcEngine.pb.h>

#pragma once

using asio::ip::tcp;

namespace rpcserver {
	bool read_byte(tcp::socket& sock, unsigned char* byte);
	bool read_int16(tcp::socket& sock, uint16_t* out);
	bool read_int32(tcp::socket& sock, uint32_t* out);
	bool read_int64(tcp::socket& sock, uint64_t* out);
	size_t read_varint(tcp::socket& sock, uint64_t* out);
	std::string read_string(tcp::socket& sock);
	bool read_proto(tcp::socket& sock, ::google::protobuf::Message& proto,
uint64_t *consumed);
	bool read_proto(tcp::socket& sock, ::google::protobuf::Message& proto);
}
