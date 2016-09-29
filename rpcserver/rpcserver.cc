#include <iostream>
#include <asio.hpp>
#include <thread>

#include <RpcHeader.pb.h>
#include <ProtobufRpcEngine.pb.h>
#include <IpcConnectionContext.pb.h>

#include "socket_reads.cc"
#include "rpcserver.h"

#define ERROR_AND_RETURN(msg) ::std::cerr << msg << ::std::endl; return
#define ERROR_AND_FALSE(msg) ::std::cerr << msg << ::std::endl; return false

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
        std::cout << "Got prelude length: " << prelude_len << std::endl;
    } else {
        ERROR_AND_FALSE("Failed to receive message prelude length.");
    }
    hadoop::common::RpcRequestHeaderProto rpc_request_header;
    if (read_proto(sock, rpc_request_header)) {
        std::cout << rpc_request_header.DebugString() << std::endl;
    } else {
        ERROR_AND_FALSE("Couldn't read the request header");
    }
    // Now read the length of the IpcConnectionContext and deserialize.
    hadoop::common::IpcConnectionContextProto connection_context;
    if (read_proto(sock, connection_context)) {
        std::cout << connection_context.DebugString() << std::endl;
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
        std::cout << "Got handshake version=" << version << ", service=" << service
                  << " protocol=" << auth_protocol << std::endl;
    } else {
        ERROR_AND_RETURN("Failed to receive handshake.");
    }
    if (receive_prelude(sock)) {
        std::cout << "Received whole prelude." << std::endl;
    } else {
        ERROR_AND_RETURN("Failed to receive prelude.");
    }
    for (;;) {
        // Main listen loop for RPC commands.
        uint32_t payload_size;
        if (!read_int32(sock, &payload_size)) {
            ERROR_AND_RETURN("Failed to payload size.");
        }
        std::cout << "Got payload size: " << payload_size << std::endl;
        hadoop::common::RpcRequestHeaderProto rpc_request_header;
        if (!read_proto(sock, rpc_request_header)) {
            ERROR_AND_RETURN("Failed to read the RPC request header");
        }
        std::cout << rpc_request_header.DebugString() << std::endl;

        hadoop::common::RequestHeaderProto request_header;
        if (!read_proto(sock, request_header)) {
            ERROR_AND_RETURN("Failed to read the request header");
        }
        std::cout << request_header.DebugString() << std::endl;

        uint64_t request_len;
        if (read_varint(sock, &request_len) == 0) {
            ERROR_AND_RETURN("Request length was 0?");
        }
        std::cout << "request length=" << request_len << std::endl;
        std::string request(request_len, 0);
        size_t rcv_len = sock.read_some(asio::buffer(&request[0], request_len), error);
        if (rcv_len != request_len || error) {
            ERROR_AND_RETURN("Failed to receive request.");
        }
        std::cout << request << std::endl;

        // TODO: dispatch on request_header.methodName.
        auto iter = this->dispatch_table.find(request_header.methodname());
        if (iter != this->dispatch_table.end()) {
            std::cout << "dispatching handler for " << request_header.methodname() << std::endl;
            iter->second(request);
        } else {
            std::cout << "no handler found for " << request_header.methodname() << std::endl;
        }
    }
}

void RPCServer::register_handler(std::string key, std::function<std::string(std::string)> handler) {
    this->dispatch_table[key] = handler;
}

void RPCServer::serve(asio::io_service& io_service) {
    std::cout << "Listen on :" << this->port << std::endl;
    tcp::acceptor a(io_service, tcp::endpoint(tcp::v4(), this->port));

    for (;;) {
        tcp::socket sock(io_service);
        a.accept(sock);
        std::thread(&RPCServer::handle_rpc, this, std::move(sock)).detach();
    }
}


#undef ERROR_AND_RETURN
#undef ERROR_AND_FALSE
