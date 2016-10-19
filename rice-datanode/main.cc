#define ELPP_FRESH_LOG_FILE
#define ELPP_THREAD_SAFE

#include <cstdlib>
#include <iostream>
#include <asio.hpp>
#include <rpcserver.h>
#include <easylogging++.h>
#include "ClientDatanodeProtocolImpl.h"

// initialize the logging library (only do this once!)
INITIALIZE_EASYLOGGINGPP

#define LOG_CONFIG_FILE "dn-log-conf.conf"

using namespace client_datanode_translator;

int main(int argc, char* argv[]) {
	//el::Configurations conf(LOG_CONFIG_FILE);
	//el::Loggers::reconfigureAllLoggers(conf);

	asio::io_service io_service;
	unsigned short port = 5544;
	if (argc == 2) {
		port = std::atoi(argv[1]);
	}
	ClientDatanodeTranslator translator(port);
	translator.getRPCServer().serve(io_service);
}
