#include <iostream>
#include "ClientNamenodeProtocol.pb.h"
#include "hdfs.pb.h"
#include <google/protobuf/message.h>
#include <rpcserver.h>
#include <ConfigReader.h>
#include "Leases.h"

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
		~ClientNamenodeTranslator();

		std::string getFileInfo(std::string);
		std::string mkdir(std::string);
		std::string destroy(std::string);
		std::string create(std::string);
		std::string getBlockLocations(std::string);
		std::string getServerDefaults(std::string);
		std::string renewLease(std::string);
		std::string complete(std::string);
		std::string setReplication(std::string);

		// RPC calls which are not supported
		std::string rename(std::string);
		std::string rename2(std::string);
		std::string append(std::string);
		std::string setPermission(std::string);
		std::string recoverLease(std::string);
		std::String concat(std::string);

		// lease manager interactions

		int getPort();
		RPCServer getRPCServer();
	private:
		std::string Serialize(google::protobuf::Message&);
		void InitServer();
		void RegisterClientRPCHandlers();
		void logMessage(google::protobuf::Message& req, std::string req_name);
		void leaseCheck();
		
		int getDefaultInt(std::string);

        static const int LEASE_CHECK_TIME;
		lease::LeaseManager lease_manager; 
		config_reader::ConfigReader config; 
		int port;
		RPCServer server;
}; // class
} // namespace
