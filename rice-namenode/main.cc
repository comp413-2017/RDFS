#include <cstdlib>
#include <asio.hpp>
#include <rpcserver.h>

int main(int argc, char* argv[]) {
    asio::io_service io_service;
    short port = 5351;
    if (argc == 2) {
        port = std::atoi(argv[1]);
    }
    RPCServer server(port);
    server.serve(io_service);
}
