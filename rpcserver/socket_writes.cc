#include <iostream>
#include <asio.hpp>
#include <netinet/in.h>
#include <google/protobuf/io/coded_stream.h>

using asio::ip::tcp;

namespace rpcserver {

    /**
     * Return whether an attempt to write val on socket is successful.
     */
    bool write_int32(tcp::socket& sock, uint32_t val) {
        asio::error_code error;
        size_t write_len = sock.write_some(asio::buffer(&val, 4), error);
        return !error && write_len == 4;
    }

    /**
     * Return whether an attempt to write given value as a varint on socket is
     * successful.
     */
    bool write_varint(tcp::socket& sock, uint64_t val) {
        asio::error_code error;
        size_t len = ::google::protobuf::io::CodedOutputStream::VarintSize64(val);
        uint8_t* buf = new uint8_t[len];
        ::google::protobuf::io::CodedOutputStream::WriteVarint64ToArray(val, buf);
        size_t write_len = sock.write_some(asio::buffer(buf, len), error);
        delete[] buf;
        return !error && write_len == len;
    }

    /**
     * Return success of attempt to write delimited (length + message) proto on
     * socket.
     */
    bool write_delimited_proto(tcp::socket& sock, std::string& proto_bytes) {
        asio::error_code error;
        if (proto_bytes.size() == 0) {
            return true;
        }
        if (!write_varint(sock, proto_bytes.size())) {
            return false;
        }
        size_t write_len = sock.write_some(asio::buffer(&proto_bytes[0],
                                                        proto_bytes.size()), error);
        return write_len == proto_bytes.size() && !error;
    }

}
