#include <iostream>
#include <asio.hpp>
#include <thread>

#include <RpcHeader.pb.h>
#include <ProtobufRpcEngine.pb.h>
#include <IpcConnectionContext.pb.h>

#include <easylogging++.h>

#include "socket_writes.cc"
#include "socket_reads.cc"
#include "rpcserver.h"

#define ERROR_AND_RETURN(msg) LOG(ERROR) << msg; return
#define ERROR_AND_FALSE(msg) LOG(ERROR) << msg; return false

using asio::ip::tcp;

using namespace rpcserver;

RPCServer::RPCServer(int p) : port{p} {}

/**
 * Return true on success, false on failure. On success, version, service,
 * and auth protocol set from the received
 */
bool RPCServer::receive_handshake(tcp::socket& sock, short* version, short* service, short* auth_protocol) {
    // handshake header.
    // Handshake has 7 bytes.
    constexpr size_t handshake_len = 7;
    asio::error_code error;
    char data[handshake_len];
    size_t rec_len = sock.read_some(asio::buffer(data, handshake_len), error);
    if (!error && rec_len == handshake_len) {
        // First 4 bytes are 'hrpc'
        if (data[0] == 'h' && data[1] == 'r' && data[2] == 'p' && data[3] == 'c') {
            *version = data[4];
            *service = data[5];
            *auth_protocol = data[6];
            return true;
        }
    }
    return false;
}

/**
 * Attempt to receive the RPC prelude from the socket.
 * Return whether the attempt was succesful.
 */
bool RPCServer::receive_prelude(tcp::socket& sock) {
    // Attempt to receive the RPC prelude (header + context) from the socket.
    asio::error_code error;
    uint32_t prelude_len;
    if (read_int32(sock, &prelude_len)) {
        LOG(INFO) << "Got prelude length: " << prelude_len;
    } else {
        ERROR_AND_FALSE("Failed to receive message prelude length.");
    }
    hadoop::common::RpcRequestHeaderProto rpc_request_header;
    if (read_proto(sock, rpc_request_header)) {
        LOG(INFO) << rpc_request_header.DebugString();
    } else {
        ERROR_AND_FALSE("Couldn't read the request header");
    }
    // Now read the length of the IpcConnectionContext and deserialize.
    hadoop::common::IpcConnectionContextProto connection_context;
    if (read_proto(sock, connection_context)) {
        LOG(INFO) << connection_context.DebugString();
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
    short version, service, auth_protocol;
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
            ERROR_AND_RETURN("Failed to read payload size, maybe connection closed.");
        }
        LOG(INFO) << "Got payload size: " << payload_size;
        hadoop::common::RpcRequestHeaderProto rpc_request_header;
        if (!read_proto(sock, rpc_request_header)) {
            ERROR_AND_RETURN("Failed to read the RPC request header");
        }
        LOG(INFO) << rpc_request_header.DebugString();

        hadoop::common::RequestHeaderProto request_header;
        if (!read_proto(sock, request_header)) {
            ERROR_AND_RETURN("Failed to read the request header");
        }
        LOG(INFO) << request_header.DebugString();

        uint64_t request_len;
        if (read_varint(sock, &request_len) == 0) {
            ERROR_AND_RETURN("Request length was 0?");
        }
        LOG(INFO) << "request length=" << request_len;
        std::string request(request_len, 0);
        size_t rcv_len = sock.read_some(asio::buffer(&request[0], request_len), error);
        if (rcv_len != request_len || error) {
            ERROR_AND_RETURN("Failed to receive request.");
        }
        auto iter = this->dispatch_table.find(request_header.methodname());
        if (iter != this->dispatch_table.end()) {
            LOG(INFO) << "dispatching handler for " << request_header.methodname();
            // Send the response back on the socket.
            hadoop::common::RpcResponseHeaderProto response_header;
            response_header.set_callid(rpc_request_header.callid());
            response_header.set_status(hadoop::common::RpcResponseHeaderProto_RpcStatusProto_SUCCESS);
            response_header.set_clientid(rpc_request_header.clientid());
            std::string response_header_str;
            response_header.SerializeToString(&response_header_str);
            std::string response = iter->second(request);
            if (write_int32(sock, response.size() + response_header_str.size()) &&
                write_delimited_proto(sock, response_header_str) &&
                write_delimited_proto(sock, response)) {
                LOG(INFO)  << "successfully wrote response to client.";
            } else {
                LOG(INFO) << "failed to write response to client.";
            }
        } else {
            LOG(INFO) << "no handler found for " << request_header.methodname();
        }
    }
}

/**
 * Register given handler function on the provided method key.
 * The function's input will be a string of bytes corresponding to the Proto of
 * its input. Exepct the function to return a string of serialized bytes of its
 * specified output Proto.
 */
void RPCServer::register_handler(std::string key, std::function<std::string(std::string)> handler) {
    this->dispatch_table[key] = handler;
}

/**
 * Begin the server's main listen loop using provided io service.
 */
void RPCServer::serve(asio::io_service& io_service) {
    LOG(INFO) << "Listen on :" << this->port;
    tcp::acceptor a(io_service, tcp::endpoint(tcp::v4(), this->port));

    for (;;) {
        tcp::socket sock(io_service);
        a.accept(sock);
        std::thread(&RPCServer::handle_rpc, this, std::move(sock)).detach();
    }
}


#undef ERROR_AND_RETURN
#undef ERROR_AND_FALSE
