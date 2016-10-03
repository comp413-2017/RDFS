#include <cstdlib>
#include <iostream>
#include <asio.hpp>
#include <rpcserver.h>
#include "ClientNamenodeProtocolImpl.h"

using namespace client_namenode_translator;

int main(int argc, char* argv[]) {
    asio::io_service io_service;
    short port = 5351;
    if (argc == 2) {
        port = std::atoi(argv[1]);
    }
	ClientNamenodeTranslator translator(port);
	translator.getRPCServer().serve(io_service);
}
