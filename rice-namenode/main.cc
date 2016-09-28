#include <cstdlib>
#include <array>
#include <thread>
#include <iostream>
#include <asio.hpp>
#include <RpcHeader.pb.h>
#include <IpcConnectionContext.pb.h>
#include "socket_reads.cc"

using asio::ip::tcp;

#define ERROR_AND_FALSE(msg) ::std::cerr << msg << ::std::endl; return false

/**
 * Return true on success, false on failure. On success, version, service,
 * and auth protocol set from the received
 */
static bool receive_handshake(tcp::socket& sock, short* version, short* service, short* auth_protocol) {
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
static bool receive_prelude(tcp::socket& sock) {
    // Attempt to receive the RPC prelude (header + context) from the socket.
    asio::error_code error;
    uint32_t prelude_len;
    if (read_int32(sock, &prelude_len)) {
        std::cout << "Got prelude length: " << prelude_len << std::endl;
    } else {
        ERROR_AND_FALSE("Failed to receive message prelude length.");
    }
    char* prelude = new char[prelude_len];
    uint64_t header_len;
    sock.read_some(asio::buffer(prelude, prelude_len), error);
    std::string prelude_str(prelude, prelude_len);
    if (error) {
        ERROR_AND_FALSE("Failed to read message prelude.");
    }
    uint64_t offset;
    hadoop::common::RpcRequestHeaderProto rpc_request_header;
    if (read_proto(prelude, prelude_len, rpc_request_header, &offset)) {
        std::cout << rpc_request_header.DebugString() << std::endl;
    } else {
        ERROR_AND_FALSE("Couldn't read the request header");
    }
    // Now read the length of the IpcConnectionContext and deserialize.
    hadoop::common::IpcConnectionContextProto connection_context;
    if (read_proto(prelude + offset, prelude_len - offset, connection_context)) {
        std::cout << connection_context.DebugString() << std::endl;
    } else {
        ERROR_AND_FALSE("Couldn't read the ipc connection context");
    }
    delete[] prelude;
    return true;
}

/**
 * Handle a single RPC connection.
 */
static void handle_rpc(tcp::socket sock) {
    // Remark: No need to close socket, it happens automatically in its
    // destructor.
    short version, service, auth_protocol;
    if (receive_handshake(sock, &version, &service, &auth_protocol)) {
        std::cout << "Got handshake version= " << version << ", service=" << service
                  << " protocol=" << auth_protocol << std::endl;
    } else {
        std::cout << "Failed to receive handshake." << std::endl;
    }
    if (receive_prelude(sock)) {
        std::cout << "Received whole prelude." << std::endl;
    } else {
        std::cerr << "Error on receiving prelude." << std::endl;
    }
}

int main(int argc, char* argv[]) {
    asio::io_service io_service;
    short port = 5351;
    if (argc == 2) {
        port = std::atoi(argv[1]);
    }
    std::cout << "Listen on :" << port << std::endl;

    tcp::acceptor a(io_service, tcp::endpoint(tcp::v4(), port));

    for (;;) {
        tcp::socket sock(io_service);
        a.accept(sock);
        std::thread(handle_rpc, std::move(sock)).detach();
    }
}
