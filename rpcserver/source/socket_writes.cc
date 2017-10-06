// Copyright 2017 Rice University, COMP 413 2017

#include <easylogging++.h>
#include <netinet/in.h>
#include <google/protobuf/io/coded_stream.h>
#include <iostream>
#include "socket_writes.h"
#include <asio.hpp>

using asio::ip::tcp;

namespace rpcserver {

/**
 * Return whether an attempt to write val on socket is successful.
 */
bool write_int32(tcp::socket &sock, uint32_t val) {
  asio::error_code error;
  uint32_t val_net = htonl(val);
  size_t write_len = sock.write_some(asio::buffer(&val_net, 4), error);
  return !error && write_len == 4;
}

/**
 * Return whether an attempt to write 16-bit val on socket is successful.
 */
bool write_int16(tcp::socket &sock, uint16_t val) {
  asio::error_code error;
  uint16_t val_net = htons(val);
  size_t write_len = sock.write_some(asio::buffer(&val_net, 2), error);
  return !error && write_len == 2;
}

/**
 * Return whether an attempt to write 8-bit val on socket is successful.
 */
bool write_byte(tcp::socket &sock, unsigned char byte) {
  asio::error_code error;
  uint8_t val_net = byte;
  size_t write_len = sock.write_some(asio::buffer(&val_net, 1), error);
  return !error && write_len == 1;
}

/**
 * Return whether an attempt to write given value as a varint on socket is
 * successful.
 */
bool write_varint(tcp::socket &sock, uint32_t val) {
  asio::error_code error;
  size_t len = ::google::protobuf::io::CodedOutputStream::VarintSize64(val);
  uint8_t *buf = new uint8_t[len];
  ::google::protobuf::io::CodedOutputStream::WriteVarint32ToArray(val, buf);
  size_t write_len = sock.write_some(asio::buffer(buf, len), error);
  delete[] buf;
  if (error)
    LOG(ERROR) << error;
  if (write_len != len)
    LOG(INFO) << write_len << " " << len;
  return !error && write_len == len;
}

/**
 * Return success of attempt to write delimited (length + message) proto on
 * socket.
 */
bool write_delimited_proto(tcp::socket &sock, std::string &proto_bytes) {
  if (!write_varint(sock, proto_bytes.size())) {
    LOG(ERROR) << "varint failed" << proto_bytes.size() << std::endl;
    return false;
  }
  return write_proto(sock, proto_bytes);
}

/**
 * Return success of attempt to write proto on socket.
 */
bool write_proto(tcp::socket &sock, std::string &proto_bytes) {
  asio::error_code error;
  size_t write_len = sock.write_some(asio::buffer(&proto_bytes[0],
                                                  proto_bytes.size()), error);
  if (write_len != proto_bytes.size())
    LOG(ERROR) << "write_len "
               << write_len
               << "protob size "
               << proto_bytes.size()
               << std::endl;

  if (error)
    LOG(ERROR) << "sock.write_some returned error" << std::endl;

  return write_len == proto_bytes.size() && !error;
}
}  // namespace rpcserver
