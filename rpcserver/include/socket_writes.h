#include <iostream>
#include <asio.hpp>
#include <netinet/in.h>

#include <ProtobufRpcEngine.pb.h>

#pragma once

using asio::ip::tcp;

namespace rpcserver {
    bool write_int32(tcp::socket& sock, uint32_t val);
    bool write_int16(tcp::socket& sock, uint16_t val);
    bool write_varint(tcp::socket& sock, uint64_t val);
    bool write_proto(tcp::socket& sock, std::string& proto_bytes);
    bool write_delimited_proto(tcp::socket& sock, std::string& proto_bytes);
}
