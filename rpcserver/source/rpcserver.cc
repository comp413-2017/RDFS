// Copyright 2017 Rice University, COMP 413 2017

#include "rpcserver.h"

#define ERROR_AND_RETURN(msg) LOG(ERROR) <<  msg; return
#define ERROR_AND_FALSE(msg) LOG(ERROR) <<  msg; return false

using asio::ip::tcp;

using rpcserver::read_full;
using rpcserver::read_int32;
using rpcserver::read_varint;
using rpcserver::read_delimited_proto;
using rpcserver::write_delimited_proto;
using rpcserver::write_int32;
using rpcserver::write_varint;

RPCServer::RPCServer(int p) : port{p} {}

/**
 * Return true on success, false on failure. On success, version, service,
 * and auth protocol set from the received
 */
bool RPCServer::receive_handshake(
    tcp::socket &sock,
    int16_t *version,
    int16_t *service,
    int16_t *auth_protocol
) {
  // handshake header.
  // Handshake has 7 bytes.
  constexpr size_t kHandshakeLen = 7;
  char data[kHandshakeLen];
  asio::error_code error = read_full(sock, asio::buffer(data, kHandshakeLen));
  if (!error) {
    // First 4 bytes are 'hrpc'
    if (data[0] == 'h' && data[1] == 'r' && data[2] == 'p' && data[3] == 'c') {
      *version = data[4];
      *service = data[5];
      *auth_protocol = data[6];
      return true;
    }
  } else {
    LOG(ERROR) << "Handshake receipt error code " << error << ".";
  }
  return false;
}

/**
 * Attempt to receive the RPC prelude from the socket.
 * Return whether the attempt was succesful.
 */
bool RPCServer::receive_prelude(tcp::socket &sock) {
  // Attempt to receive the RPC prelude (header + context) from the socket.
  asio::error_code error;
  uint32_t prelude_len;
  if (read_int32(sock, &prelude_len)) {
    LOG(INFO) << "Got prelude length: " << prelude_len;
  } else {
    ERROR_AND_FALSE("Failed to receive message prelude length.");
  }
  hadoop::common::RpcRequestHeaderProto rpc_request_header;
  if (read_delimited_proto(sock, rpc_request_header)) {
    // TODO(2016): RDFS-2016 was just printing the request header here, we
    // should maybe do something else.
  } else {
    ERROR_AND_FALSE("Couldn't read the request header");
  }
  // Now read the length of the IpcConnectionContext and deserialize.
  hadoop::common::IpcConnectionContextProto connection_context;
  if (read_delimited_proto(sock, connection_context)) {
    // TODO(2016): RDFS-2016 was just printing the context here, we should
    // maybe do something else.
  } else {
    ERROR_AND_FALSE("Couldn't read the ipc connection context");
  }
  return true;
}

/**
 * Handle a single RPC connection from Hadoop's ProtobufRpcEngine.
 */
void RPCServer::handle_rpc(tcp::socket sock) {
  // Remark: No need to close socket, it happens automatically in its
  // destructor.
  asio::error_code error;
  int16_t version, service, auth_protocol;
  if (receive_handshake(sock, &version, &service, &auth_protocol)) {
    LOG(INFO) << "Got handshake version=" << version << ", service=" << service
              << " protocol=" << auth_protocol;
  } else {
    ERROR_AND_RETURN("Failed to receive handshake.");
  }
  if (receive_prelude(sock)) {
    LOG(INFO) << "Received whole prelude.";
  } else {
    ERROR_AND_RETURN("Failed to receive prelude.");
  }
  for (;;) {
    // Main listen loop for RPC commands.
    uint32_t payload_size;
    if (!read_int32(sock, &payload_size)) {
      LOG(WARNING) << "Failed to read payload size, maybe connection closed.";
      return;
    }
    LOG(INFO) << "Got payload size: " << payload_size;
    hadoop::common::RpcRequestHeaderProto rpc_request_header;
    if (!read_delimited_proto(sock, rpc_request_header)) {
      ERROR_AND_RETURN("Failed to read the RPC request header");
    }
    // TODO(2016): RDFS-2016 was just printing the request header here, we
    // should maybe do something else.

    hadoop::common::RequestHeaderProto request_header;
    if (!read_delimited_proto(sock, request_header)) {
      ERROR_AND_RETURN("Failed to read the request header");
    }
    // TODO(2016): RDFS-2016 was just printing the message here, we should
    // maybe do something else.

    uint64_t request_len;
    if (read_varint(sock, &request_len) == 0) {
      ERROR_AND_RETURN("Request length was 0?");
    }
    LOG(INFO) << "request length=" << request_len;
    std::string request(request_len, 0);
    error = read_full(sock, asio::buffer(&request[0], request_len));
    if (error) {
      ERROR_AND_RETURN("Failed to receive request.");
    }
    auto iter = dispatch_table.find(request_header.methodname());
    LOG(INFO) << "RPC_METHOD_FOUND: [[" << request_header.methodname()
              << "]]\n";

    // The if-statement is true only if there is a corresponding handler in
    // for the requested command in on of the Namenode or datanode
    // ProtocolImpl.cc files.
    std::string response_header_str;
    bool write_success;
    if (iter != dispatch_table.end()) {
      LOG(INFO) << "dispatching handler for " << request_header.methodname();
      // Send the response back on the socket.
      hadoop::common::RpcResponseHeaderProto response_header;
      response_header.set_callid(rpc_request_header.callid());
      response_header.set_clientid(rpc_request_header.clientid());
      size_t response_varint_size;
      try {
        std::string response = iter->second(request);
        response_header.set_status(
            hadoop::common::RpcResponseHeaderProto_RpcStatusProto_SUCCESS);
        response_varint_size =
            ::google::protobuf::io::CodedOutputStream::VarintSize32(
                response.size());
        size_t header_varint_size =
            ::google::protobuf::io::CodedOutputStream::VarintSize32(
                response_header_str.size());
        response_header.SerializeToString(&response_header_str);
        write_success =
            write_int32(
                sock,
                response_varint_size +
                    response.size() +
                    header_varint_size +
                    response_header_str.size()) &&
            write_delimited_proto(sock, response_header_str) &&
            write_delimited_proto(sock, response);
      } catch (hadoop::common::RpcResponseHeaderProto &response_header) {
        // Some sort of failure occurred in our command's handler.
        LOG(INFO) << "Returning error rpc header to client";
        write_success = send_error_header(
            rpc_request_header,
            response_header,
            response_header_str,
            sock);
      }
      if (write_success) {
        LOG(INFO) << "successfully wrote response to client.";
      } else {
        LOG(ERROR) << "failed to write response to client.";
      }
    } else {
      // In this case, we see that the lookup for a function to handle the
      // requested command (for example, see ClientNamenodeProtocolImpl)  using
      // the dispatch table failed. As such, all we must send is a
      // header that specifices no handler method for this command was found.
      LOG(INFO) << "NO HANDLER FOUND FOR " << request_header.methodname();
      hadoop::common::RpcResponseHeaderProto response_header;
      response_header.set_status(
          hadoop::common::
          RpcResponseHeaderProto_RpcStatusProto_ERROR);
      response_header.set_errormsg("No handler found for requested command");
      response_header.set_exceptionclassname("");
      response_header.set_errordetail(
          hadoop::common::
          RpcResponseHeaderProto_RpcErrorCodeProto_ERROR_NO_SUCH_METHOD);

      LOG(INFO) << "Returning error rpc header to client since no handler "
          "found";
      // Same exact process to send it as when error occured in a handler
      write_success = send_error_header(
          rpc_request_header,
          response_header,
          response_header_str,
          sock);

      if (write_success) {
        LOG(INFO) << "successfully wrote error response to client.";
      } else {
        LOG(ERROR) << "failed to write error response to client.";
      }
    }
  }
}

/**
 * Helper function to send an error response header
 *
 * This logic is used both when there was an error in a command handler
 * as well as the case that we get a command that is unsupported
 * (meaning we don't have a command handler for said command)
 */
bool RPCServer::send_error_header(
    hadoop::common::RpcRequestHeaderProto rpc_request_header,
    hadoop::common::RpcResponseHeaderProto response_header,
    std::string response_header_str,
    tcp::socket &sock) {
  bool write_success;
  response_header.set_callid(rpc_request_header.callid());
  response_header.set_clientid(rpc_request_header.clientid());
  response_header.SerializeToString(&response_header_str);
  size_t header_varint_size =
      ::google::protobuf::io::CodedOutputStream::VarintSize32(
          response_header_str.size());
  write_success =
      write_int32(sock, header_varint_size + response_header_str.size()) &&
      write_delimited_proto(sock, response_header_str);
  return write_success;
}

/**
 * Register given handler function on the provided method key.
 * The function's input will be a string of bytes corresponding to the Proto of
 * its input. Exepct the function to return a string of serialized bytes of its
 * specified output Proto.
 */
void RPCServer::register_handler(
    std::string key,
    std::function<std::string(std::string)> handler
) {
  this->dispatch_table[key] = handler;
}

/**
 * Begin the server's main listen loop using provided io service.
 */
void RPCServer::serve(asio::io_service &io_service) {
  LOG(INFO) << "Listen on :" << this->port;
  tcp::acceptor a(io_service, tcp::endpoint(tcp::v4(), this->port));

  for (;;) {
    tcp::socket sock(io_service);
    a.accept(sock);
    std::thread(&RPCServer::handle_rpc, this, std::move(sock)).detach();
  }
}

/**
 * Returns the current user's name.
 */
std::string RPCServer::getUsername() {
  char *user = getlogin();

  if (user == NULL) {
    LOG(ERROR) << "failed to acquire user's name";
    return std::string();
  } else {
    LOG(DEBUG) << "the received user name is " << user;
    return std::string (user);
  }
}

#undef ERROR_AND_RETURN
#undef ERROR_AND_FALSE
