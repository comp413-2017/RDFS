#include <iostream>
#include "ClientDatanodeProtocol.pb.h"
#include "hdfs.pb.h"
#include <google/protobuf/message.h>
#include <rpcserver.h>
#include <ConfigReader.h>

#pragma once

/**
 * The implementation of the rpc calls. 
 */
namespace client_datanode_translator {

// the .proto file implementation's namespace, used for messages
using namespace hadoop::hdfs;

class ClientDatanodeTranslator {
	public:
		ClientDatanodeTranslator(int port);
		std::string getReplicaVisibleLength(std::string);
		std::string refreshNamenodes(std::string);
		std::string deleteBlockPool(std::string);
		std::string getBlockLocalPathInfo(std::string);
		std::string getHdfsBlockLocations(std::string);
		std::string shutdownDatanode(std::string);
		std::string getDatanodeInfo(std::string);

		int getPort();
		RPCServer getRPCServer();
	private:
		std::string Serialize(google::protobuf::Message&);
		void InitServer();
		void RegisterClientRPCHandlers();
		void Config();
		void logMessage(google::protobuf::Message& req, std::string req_name);
		int getDefaultInt(std::string);

		FsServerDefaultsProto server_defaults;
		int port;
		RPCServer server;
		config_reader::ConfigReader config;
}; // class
} // namespace
