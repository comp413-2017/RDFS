#include <cstdlib>
#include <iostream>
#include <asio.hpp>
#include <rpcserver.h>

std::string getFileInfoDemo(std::string request) {
    std::cout << "Handling getFileInfo on " << request << "!" << std::endl;
    return "Some placeholder values...";
}

int main(int argc, char* argv[]) {
    asio::io_service io_service;
    short port = 5351;
    if (argc == 2) {
        port = std::atoi(argv[1]);
    }
    RPCServer server(port);
    server.register_handler("getFileInfo", getFileInfoDemo);
    server.serve(io_service);
}
