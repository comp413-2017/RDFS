// Copyright 2017 Rice University, COMP 413 2017

#include <netinet/in.h>
#include <string>
#include <iostream>
#include <asio.hpp>

#include <ProtobufRpcEngine.pb.h>

#pragma once

using asio::ip::tcp;

namespace rpcserver {
/**
 * Write given 4-byte uint32 value to socket. Return true if successful,
 * otherwise false.
 */
bool write_int32(tcp::socket &sock, uint32_t val);
/**
 * Write given 2-byte uint16 value to socket. Return true if successful,
 * otherwise false.
 */
bool write_int16(tcp::socket &sock, uint16_t val);
/**
 * Write given byte uint8 value to socket. Return true if successful,
 * otherwise false.
 */
bool write_byte(tcp::socket &sock, unsigned char byte);
/**
 * Write given uint32 value to socket, encoded as a varint. Return true if
 * successful, otherwise false.
 */
bool write_varint(tcp::socket &sock, uint32_t val);
/**
 * Write given proto to the socket. Return true if successful, otherwise
 * false.
 */
bool write_proto(tcp::socket &sock, std::string &proto_bytes);
/**
 * Write given proto to the socket, prefixed by a varint representing its
 * length. Return true if successful, otherwise false.
 */
bool write_delimited_proto(tcp::socket &sock, std::string &proto_bytes);
}  // namespace rpcserver
