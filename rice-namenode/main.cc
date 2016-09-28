#include <cstdlib>
#include <netinet/in.h>
#include <array>
#include <thread>
#include <iostream>
#include <asio.hpp>
#include <RpcHeader.pb.h>
#include <IpcConnectionContext.pb.h>

using asio::ip::tcp;

static bool receive_handshake(tcp::socket& sock, short* version, short* service, short* auth_protocol) {
    // Return true on success, false on failure.
    // If we succeed, version, service, and auth protocol set from the received
    // handshake header.
    // Handshake has 7 bytes.
    constexpr size_t handshake_len = 7;
    asio::error_code error;
    tcp::socket::message_flags flags;
    char data[handshake_len];
    size_t rec_len = sock.receive(asio::buffer(data, handshake_len), flags, error);
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

static bool readint32(tcp::socket& sock, uint32_t* out) {
    // Attempt to read uint32 from provided socket. Consumes 4 bytes. Return
    // false means failure. Otherwise return true and set *out.
    uint32_t data;
    asio::error_code error;
    tcp::socket::message_flags flags;
    size_t rec_len = sock.receive(asio::buffer(&data, 4), flags, error);
    if (!error && rec_len == 4) {
        *out = ntohl(data);
        return true;
    }
    return false;
}

/**
 * Given socket sock and pointer to uint32 out, attempt to read a
 * variable-length integer from sock. If successful, set *out to the value of
 * the integer and return true. Otherwise return false.
 */
static bool readvarint32(tcp::socket& sock, uint32_t* out) {
    return false;
}

static bool receive_prelude(tcp::socket& sock) {
    // Attempt to receive the RPC prelude (header + context) from the socket.
    uint32_t payload_len;
    if (readint32(sock, &payload_len)) {
        std::printf("Got payload length: %d\n", payload_len);
    } else {
        std::cout << "Failed to receive payload length." << std::endl;
    }
    asio::error_code error;
    tcp::socket::message_flags flags;
    uint32_t header_len;
    if (readvarint32(sock, &header_len)) {
        std::printf("Got payload length: %d\n", header_len);
    } else {
        std::cout << "Failed to receive varint RpcHeader length." << std::endl;
    }
}

static void handle_rpc(tcp::socket sock) {
    // Remark: No need to close socket, it happens automatically in its
    // destructor.
    short version, service, auth_protocol;
    if (receive_handshake(sock, &version, &service, &auth_protocol)) {
        std::printf("Got handshake: version=%d, service=%d, protocol=%d\n", version, service, auth_protocol);
    } else {
        std::cout << "Failed to receive handshake." << std::endl;
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
