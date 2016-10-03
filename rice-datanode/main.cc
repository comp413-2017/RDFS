#include <cstdlib>
#include <iostream>
#include <asio.hpp>
#include <rpcserver.h>
#include "ClientDatanodeProtocolImpl.h"

using namespace client_datanode_translator;

int main(int argc, char* argv[]) {
    asio::io_service io_service;
    short port = 5544;
    if (argc == 2) {
        port = std::atoi(argv[1]);
    }
	ClientDatanodeTranslator translator(port);
	translator.getRPCServer().serve(io_service);
}
