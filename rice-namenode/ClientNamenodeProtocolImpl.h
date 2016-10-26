#include <iostream>
#include "ClientNamenodeProtocol.pb.h"
#include "hdfs.pb.h"
#include <google/protobuf/message.h>
#include <rpcserver.h>
#include <zkwrapper.h>
#include <ConfigReader.h>
#include "Leases.h"
#include "zk_nn_client.h"

#pragma once

/**
 * The implementation of the rpc calls. 
 */
namespace client_namenode_translator {

// the .proto file implementation's namespace, used for messages
using namespace hadoop::hdfs;

class ClientNamenodeTranslator {
	public:
		ClientNamenodeTranslator(int port, zkclient::ZkNnClient& zk_arg);
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
		std::string addBlock(std::string input);

		// RPC calls which are not supported
		std::string rename(std::string);
		std::string rename2(std::string);
		std::string append(std::string);
		std::string setPermission(std::string);
		std::string recoverLease(std::string);
		std::string concat(std::string);

		// lease manager interactions
		std::string abandonBlock(std::string input);

		int getPort();
		RPCServer getRPCServer();
	private:
		std::string Serialize(google::protobuf::Message&);
		void InitServer();
		void RegisterClientRPCHandlers();
		void logMessage(google::protobuf::Message& req);
        std::string ZookeeperPath(const std::string &hadoopPath);
		FsServerDefaultsProto server_defaults;
		int port;
		RPCServer server;
		zkclient::ZkNnClient& zk;
		lease::LeaseManager lease_manager;
		config_reader::ConfigReader config;

		static const char* HDFS_DEFAULTS_CONFIG;
		
		void logMessage(google::protobuf::Message& req, std::string req_name);
		void leaseCheck();
		
		int getDefaultInt(std::string);

        static const int LEASE_CHECK_TIME; // in seconds, how often the namenode checks all leases
}; // class
} // namespace
