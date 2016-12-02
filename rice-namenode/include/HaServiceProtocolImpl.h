#include <iostream>
#include "HAServiceProtocol.pb.h"
#include "hdfs.pb.h"
#include <google/protobuf/message.h>
#include <RpcHeader.pb.h>
#include <rpcserver.h>
#include <zkwrapper.h>
#include <ConfigReader.h>
#include "Leases.h"
#include "zk_nn_client.h"
//#include "DaemonFactory.h"

#pragma once
/**
 * The implementation of the rpc calls.
 */
namespace ha_service_translator {

// the .proto file implementation's namespace, used for messages
using namespace hadoop::common;

/**
 * The translator receives the rpc parameters from rpcserver. It then processes
 * the message and does whatever is necessary, returing a serializes protobuff.
 *
 * It communicates with zookeeper to construct the namespace and communicate with datanode
 */

class HaServiceTranslator {
	public:
		HaServiceTranslator(RPCServer* server_arg, zkclient::ZkNnClient& zk_arg, int port_arg);
		~HaServiceTranslator();

		// RPC calls which we support. Each take a string which comes form
		// the rpc call, and it is then deserialized into their proto msg
		std::string transitionToActive(std::string);
		std::string getServiceStatus(std::string);

	private:
		std::string Serialize(google::protobuf::Message&);

		/**
		 * Register all the methods with the server that handle RPC calls
		 */
		void RegisterServiceRPCHandlers();


		// FsServerDefaultsProto server_defaults; 	//server defaults as read from the config
		int port; 								// port which our rpc server is using
		RPCServer* server; 						// our rpc server
		zkclient::ZkNnClient& zk; 				// client to communicate with zookeeper
		HAServiceStateProto state;

		/**
		 * Log incoming messages "req" for rpc call "req_name"
		 */
		void logMessage(google::protobuf::Message& req, std::string req_name);

		/**
		 * Get an rpc header proto given an error message and exception classname
		 */
		hadoop::common::RpcResponseHeaderProto GetErrorRPCHeader(std::string error_msg,
        		std::string exception_classname);

        static const int LEASE_CHECK_TIME; 	// in seconds, how often the namenode checks all leases

		static const std::string CLASS_NAME;

}; // class
} // namespace
