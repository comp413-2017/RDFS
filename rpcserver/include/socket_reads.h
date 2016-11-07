#include <iostream>
#include <asio.hpp>
#include <netinet/in.h>

#include <ProtobufRpcEngine.pb.h>

#pragma once

using asio::ip::tcp;

namespace rpcserver {
	/**
	 * Read a single byte from given socket and write to *byte.
	 * Return true if successful, otherwise false.
	 */
	bool read_byte(tcp::socket& sock, unsigned char* byte);
	/**
	 * Read two bytes from given socket and write *out as an uint16.
	 * Return true if successful, otherwise false.
	 */
	bool read_int16(tcp::socket& sock, uint16_t* out);
	/**
	 * Read four bytes from given socket and write *out as an uint32.
	 * Return true if successful, otherwise false.
	 */
	bool read_int32(tcp::socket& sock, uint32_t* out);
	/**
	 * Read 8 bytes from given socket and write *out as an uint64.
	 * Return true if successful, otherwise false.
	 */
	bool read_int64(tcp::socket& sock, uint64_t* out);
	/**
	 * Read variable number of bytes from given socket and write *out as an
	 * uint64. Return the number of bytes read.
	 */
	size_t read_varint(tcp::socket& sock, uint64_t* out);
	/**
	 * Read a varint prefix-delimited proto from socket and parse into proto.
	 * Set *consumed to the number of bytes read. Return true on success,
	 * otherwise false.
	 */
	bool read_delimited_proto(tcp::socket& sock, ::google::protobuf::Message& proto,
			uint64_t *consumed);
	/**
	 * Read a varint prefix-delimited proto from socket and parse into proto.
	 * Return true on success, otherwise false.
	 */
	bool read_delimited_proto(tcp::socket& sock, ::google::protobuf::Message& proto);
	/**
	 * Parse len bytes from socket into proto. Return true on success,
	 * otherwise false.
	 */
	bool read_proto(tcp::socket& sock, ::google::protobuf::Message& proto, uint64_t len);
}
