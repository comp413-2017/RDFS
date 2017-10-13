// Copyright 2017 Rice University, COMP 413 2017

#include <easylogging++.h>
#include <google/protobuf/io/coded_stream.h>
#include <thread>
#include <iostream>
#include <string>
#include <unordered_map>
#include <functional>

#include <asio.hpp>

#include <RpcHeader.pb.h>
#include <ProtobufRpcEngine.pb.h>
#include <IpcConnectionContext.pb.h>

#include "socket_writes.h"
#include "socket_reads.h"

#include <unistd.h>

#pragma once

using asio::ip::tcp;

class RPCServer {
 public:
  /**
   * Construct an RPC server that will listen on given port.
   */
  explicit RPCServer(int port);
  /**
   * Begin the listen-loop of the RPC server. This method does not return.
   */
  void serve(asio::io_service &io_service);
  /**
   * Register a handler function for the given key, such that when a
   * request for method of name key arrives, the registered handler is
   * called.
   * @param key is the method name specified in the request header.
   * @param handler takes as input a string the bytes of the method's
   *        input protobuf, and should return a string of bytes of the
   *        method's output protobuf (so the handler should parse and
   *        serialize protobufs)
   */
  void register_handler(
      std::string key,
      std::function<std::string(std::string)> handler);

 /**
  * Returns the name of the current running linux user.
  * @return A string of the current user's name
  */
 std::string getUserName();

 private:
  /**
   * Port the server will listen on when serve is called.
   */
  int port;
  /**
   * Table where we store the registered handler functions.
   */
  std::unordered_map<std::string, std::function<std::string(std::string)>>
      dispatch_table;
  /**
   * Receive a hadoop RPC handshake and write data to provided pointers.
   * True on successful read, false otherwise.
   */
  bool receive_handshake(
      tcp::socket &sock,
      int16_t *version,
      int16_t *service,
      int16_t *auth_protocol);
  /**
   * Receive the connection prelude: RpcRequestHeaderProto and
   * IpcConnectionContextProto.
   * True if successful, false otherwise.
   */
  bool receive_prelude(tcp::socket &sock);
  /**
   * Handle a single RPC connection: it only returns when the client
   * disconnects.
   */
  void handle_rpc(tcp::socket sock);
  /**
   * Helper function to send an error response header - it's used either if
   * there
  * is an error inside a command's handler, or if there isn't a handler for the
  * requested command (meaning it's unsupported)
   * True if successful, false on failure
  */
  bool send_error_header(
      hadoop::common::RpcRequestHeaderProto rpc_request_header,
      hadoop::common::RpcResponseHeaderProto response_header,
      std::string response_header_str,
      tcp::socket &sock);

  // **RPCSserver**
  static const std::string CLASS_NAME;
};
