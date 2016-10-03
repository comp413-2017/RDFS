#include <iostream>
#include "ClientNamenodeProtocol.pb.h"
#include "hdfs.pb.h"
#include <google/protobuf/message.h>
#include <rpcserver.h>

#pragma once

/**
 * The implementation of the rpc calls. 
 */
namespace client_namenode_translator {

// the .proto file implementation's namespace, used for messages
using namespace hadoop::hdfs;

class ClientNamenodeTranslator {
	public:
		ClientNamenodeTranslator(int port); 
		std::string getFileInfo(std::string);
		std::string mkdir(std::string);
		std::string append(std::string);
		std::string destroy(std::string);
		std::string create(std::string);
		std::string getBlockLocations(std::string);
		std::string getServerDefaults(std::string);

		int getPort();
		RPCServer getRPCServer();
	private:
		std::string Serialize(std::string*, google::protobuf::Message&);
		void InitServer();
		void RegisterClientRPCHandlers();
		void Config();
		void logMessage(google::protobuf::Message& req);

		FsServerDefaultsProto server_defaults;
		int port;
		RPCServer server;

		static const char* HDFS_DEFAULTS_CONFIG;
		
}; // class
} // namespace
