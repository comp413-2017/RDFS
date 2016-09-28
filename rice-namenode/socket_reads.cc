#include <iostream>
#include <asio.hpp>
#include <netinet/in.h>

using asio::ip::tcp;

namespace rpcserver {

    /**
     * Attempt to read a byte from the socket. Return true on success and set *byte
     * to its value. Otherwise return false.
     */
    bool read_byte(tcp::socket& sock, unsigned char* byte) {
        asio::error_code error;
        unsigned char read[1];
        size_t rec_len = sock.read_some(asio::buffer(read, 1), error);
        *byte = read[0];
        return !error && rec_len == 1;
    }


    /**
     * Attempt to read big-endian uint16 from provided socket. Consumes 2 bytes.
     * Return false means failure. Otherwise return true and set *out.
     */
    bool read_int16(tcp::socket& sock, uint16_t* out) {
        uint16_t data;
        asio::error_code error;
        size_t rec_len = sock.read_some(asio::buffer(&data, 2), error);
        if (!error && rec_len == 2) {
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
        asio::error_code error;
        size_t rec_len = sock.read_some(asio::buffer(&data, 4), error);
        if (!error && rec_len == 4) {
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
        asio::error_code error;
        size_t rec_len = sock.read_some(asio::buffer(&data, 8), error);
        if (!error && rec_len == 8) {
            *out = be64toh(data);
            return true;
        }
        return false;
    }


    /**
     * Attempt to read a string from provided socket. On failure, return empty
     * string.
     */
    std::string read_string(tcp::socket& sock) {
        uint16_t len;
        if (read_int16(sock, &len)) {
            asio::error_code error;
            char buf[len];
            size_t rec_len = sock.read_some(asio::buffer(buf, len), error);
            if (!error && rec_len == len) {
                return std::string(buf, len);
            }
        }
        std::string empty;
        return empty;
    }


    /**
     * Given unsigned character buffer with capacity cap, attempt to read a varint.
     * Put the value of the varint in *out, and return the number of bytes consumed.
     */
    size_t read_varint(char *buf, size_t cap, uint64_t* out) {
        size_t idx = 0;
        size_t shift = 0;
        uint64_t val = 0;
        do {
            val |= (buf[idx] & 0x7F) << shift;
            idx++;
            shift += 7;
        } while (buf[idx] & 0x80 && idx < cap);
        *out = val;
        return idx;
    }


    /**
     * Attempt to parse given protocol from provided buffer with capactiy limit.
     * Return whether the parse was successful. If successful, set *consumed to the
     * number of bytes consumed by the read.
     */
    bool read_proto(char *buf, size_t cap, ::google::protobuf::Message& proto, uint64_t *consumed) {
        uint64_t len;
        size_t skip = read_varint(buf, cap, &len);
        std::string proto_str(buf + skip, std::min(len, cap - skip));
        if (consumed != NULL) {
            *consumed = skip + len;
        }
        return proto.ParseFromString(proto_str);
    }


    /**
     * Attempt to parse given protocol from provided buffer with capactiy limit.
     * Return whether the parse was successful.
     */
    bool read_proto(char* buf, size_t cap, ::google::protobuf::Message& proto) {
        return read_proto(buf, cap, proto, NULL);
    }

}
