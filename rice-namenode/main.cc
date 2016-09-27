#include <cstdlib>
#include <thread>
#include <iostream>
#include <asio.hpp>
#include <RpcHeader.pb.h>

using asio::ip::tcp;

static bool receive_handshake(tcp::socket& sock, short* version, short* service, short* auth_protocol) {
    // Return true on success, false on failure.
    // If we succeed, version, service, and auth protocol set from the received
    // handshake header.
    return false;
}

// TODO: mkdir
static void session(tcp::socket sock) {
    constexpr size_t buflen = 1024;
    asio::error_code error;
    tcp::socket::message_flags flags;
    std::uint32_t size;
    size_t length = sock.receive(asio::buffer(&size, sizeof(size)), flags, error);
    std::cout << size << std::endl;
    for (;;) {
        char data[buflen];
        size_t length = sock.receive(asio::buffer(data, buflen), flags, error);
        if (error) {
            std::cout << "Something unexpected." << std::endl;
            break;
        }
        std::cout << length << std::endl;
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
        std::thread(session, std::move(sock)).detach();
    }
}
