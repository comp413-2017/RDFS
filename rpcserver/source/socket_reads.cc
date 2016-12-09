#include <iostream>
#include <asio.hpp>
#include <netinet/in.h>
#include "socket_reads.h"
#include <easylogging++.h>


using asio::ip::tcp;

namespace rpcserver {
    // TODO: sub namespace for these

	/**
	 * Attempt to completely overwrite given pre-allocated buffer from provided
	 * socket. Returns resulting error_code, 0 means success.
	 */
	asio::error_code read_full(tcp::socket& sock, asio::mutable_buffers_1 buf) {
		size_t size = asio::buffer_size(buf);
		// The default constructor for error_code is success.
		asio::error_code error;
		asio::read(sock, buf, asio::transfer_exactly(size), error);
		return error;
	}

    /**
     * Attempt to read a byte from the socket. Return true on success and set *byte
     * to its value. Otherwise return false.
     */
    bool read_byte(tcp::socket& sock, unsigned char* byte) {
        unsigned char read[1];
        asio::error_code error = read_full(sock, asio::buffer(read, 1));
        *byte = read[0];
        return !error;
    }


    /**
     * Attempt to read big-endian uint16 from provided socket. Consumes 2 bytes.
     * Return false means failure. Otherwise return true and set *out.
     */
    bool read_int16(tcp::socket& sock, uint16_t* out) {
        uint16_t data;
        asio::error_code error = read_full(sock, asio::buffer(&data, 2));
        if (!error) {
            *out = ntohs(data);
            return true;
        }
        return false;
    }


    /**
     * Attempt to read big-endian uint32 from provided socket. Consumes 4 bytes.
     * Return false means failure. Otherwise return true and set *out.
     */
    bool read_int32(tcp::socket& sock, uint32_t* out) {
        uint32_t data;
        asio::error_code error = read_full(sock, asio::buffer(&data, 4));
        if (!error) {
            *out = ntohl(data);
            return true;
        }
        return false;
    }


    /**
     * Attempt to read big-endian uint64 from provided socket. Consumes 8 bytes.
     * Return false means failure. Otherwise return true and set *out.
     */
    bool read_int64(tcp::socket& sock, uint64_t* out) {
        uint64_t data;
        asio::error_code error = read_full(sock, asio::buffer(&data, 8));
        if (!error) {
            *out = be64toh(data);
            return true;
        }
        return false;
    }


    /**
     * Given a socket sock, attempt to read a variable-length integer place the
     * result in *out. Return the number of bytes read.
     */
    size_t read_varint(tcp::socket& sock, uint64_t* out) {
        size_t idx = 0;
        size_t shift = 0;
        uint64_t val = 0;
        unsigned char byte;
        do {
            if (!read_byte(sock, &byte)) {
                break;
            }
            val |= (byte & 0x7F) << shift;
            idx++;
            shift += 7;
        } while (byte & 0x80);
        *out = val;
        return idx;
    }


    /**
     * Attempt to parse given protocol from provided socket.
     * Return whether the parse was successful. If successful, set *consumed to the
     * number of bytes consumed by the read.
     */
    bool read_delimited_proto(tcp::socket& sock, ::google::protobuf::Message& proto, uint64_t *consumed) {
        uint64_t len;
        size_t skip = read_varint(sock, &len);
        std::string buf(len, 0);
        asio::error_code error = read_full(sock, asio::buffer(&buf[0], len));
        if (consumed != NULL) {
            *consumed = skip + len;
        }

	if (error)
		LOG(ERROR) << "read_full returned error in delim";

        return !error && proto.ParseFromString(buf);
    }
	
	/**
     * Attempt to parse given protocol with given length from provided socket.
     * Return whether the parse was successful.
     */
    bool read_proto(tcp::socket& sock, ::google::protobuf::Message& proto, uint64_t len) {
        std::string buf(len, 0);
        asio::error_code error = read_full(sock, asio::buffer(&buf[0], len));
        return !error && proto.ParseFromString(buf);
    }
	
	
    /**
     * Attempt to parse given protocol from provided socket.
     * Return whether the parse was successful.
     */
    bool read_delimited_proto(tcp::socket& sock, ::google::protobuf::Message& proto) {
        return read_delimited_proto(sock, proto, NULL);
    }
}
