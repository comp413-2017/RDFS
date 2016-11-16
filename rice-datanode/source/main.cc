#define ELPP_FRESH_LOG_FILE
#define ELPP_THREAD_SAFE

#include <cstdlib>
#include <iostream>
#include <asio.hpp>
#include <rpcserver.h>
#include <easylogging++.h>
#include "ClientDatanodeProtocolImpl.h"
#include "data_transfer_server.h"
#include "native_filesystem.h"
#include "zk_dn_client.h"

// initialize the logging library (only do this once!)
INITIALIZE_EASYLOGGINGPP

#define LOG_CONFIG_FILE "dn-log-conf.conf"

using namespace client_datanode_translator;

int main(int argc, char* argv[]) {
	el::Configurations conf(LOG_CONFIG_FILE);
	el::Loggers::reconfigureAllLoggers(conf);

	asio::io_service io_service;
	unsigned short xferPort = 50010;
	unsigned short ipcPort = 50020;
	if (argc >= 2) {
		xferPort = std::atoi(argv[1]);
	}
	if (argc >= 3) {
		ipcPort = std::atoi(argv[2]);
	}
	auto fs = std::make_shared<nativefs::NativeFS>("/dev/sdb");
	auto dncli = std::make_shared<zkclient::ZkClientDn>("127.0.0.1", "localhost", "localhost:2181", ipcPort, xferPort); // TODO: Change the datanode id
	ClientDatanodeTranslator translator(ipcPort);
	TransferServer transfer_server(xferPort, fs, dncli);
	transfer_server.serve(io_service);
	translator.getRPCServer().serve(io_service);
}
