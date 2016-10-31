#include <iostream>
#include <string>
#include <unistd.h>

#include <google/protobuf/arena.h>
#include <google/protobuf/arenastring.h>
#include <google/protobuf/generated_message_util.h>
#include <google/protobuf/metadata.h>
#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/extension_set.h>
#include <google/protobuf/generated_enum_reflection.h>
#include <google/protobuf/unknown_field_set.h>

#include <easylogging++.h>
#include <rpcserver.h>
#include <ConfigReader.h>

#include "ClientDatanodeProtocolImpl.h"

/**
 * The implementation of the rpc calls.
 */
namespace client_datanode_translator {

// the .proto file implementation's namespace, used for messages
using namespace hadoop::hdfs;

// config
std::map <std::string, std::string> config;

// TODO - this will probably take some zookeeper object
ClientDatanodeTranslator::ClientDatanodeTranslator(int port_arg)
	: port(port_arg), server(port) {
	InitServer();
	std::cout << "Created client datanode translator." << std::endl;
}

std::string ClientDatanodeTranslator::getReplicaVisibleLength(std::string input) {
	GetReplicaVisibleLengthRequestProto req;
	req.ParseFromString(input);
	logMessage(req, "GetReplicaVisibleLength ");
	const hadoop::hdfs::ExtendedBlockProto& block = req.block();
	GetReplicaVisibleLengthResponseProto res;
	// TODO get the visible length of the block and set it the response
	//res.set_length();
	return Serialize(res);
}

std::string ClientDatanodeTranslator::refreshNamenodes(std::string input) {
	RefreshNamenodesRequestProto req;
	req.ParseFromString(input);
	logMessage(req, "RefreshNamenodes ");
	RefreshNamenodesResponseProto res;
	// TODO refresh the namenodes. Response contains no fields
	return Serialize(res);
}

std::string ClientDatanodeTranslator::deleteBlockPool(std::string input) {
	DeleteBlockPoolRequestProto req;
	req.ParseFromString(input);
	logMessage(req, "DeleteBlockPool ");
	const std::string& block_pool = req.blockpool();
	const bool force = req.force();
	DeleteBlockPoolResponseProto res;
	// TODO delete the block pool. Response contains no fields
	return Serialize(res);
}

std::string ClientDatanodeTranslator::getBlockLocalPathInfo(std::string input) {
	GetBlockLocalPathInfoRequestProto req;
	req.ParseFromString(input);
	logMessage(req, "GetBlockLocalPathInfo ");
	const hadoop::hdfs::ExtendedBlockProto& block = req.block();
	const hadoop::common::TokenProto& token = req.token();
	GetBlockLocalPathInfoResponseProto res;
	// TODO get local path info for block
	//res.set_block();
	//res.set_localpath();
	//res.set_localmetapath();
	return Serialize(res);
}

std::string ClientDatanodeTranslator::getHdfsBlockLocations(std::string input) {
	GetHdfsBlockLocationsRequestProto req;
	req.ParseFromString(input);
	logMessage(req, "GetHdfsBlockLocations ");
	const std::string& block_pool_id = req.blockpoolid();
	for (int i = 0; i < req.tokens_size(); i++) {
		const hadoop::common::TokenProto& token = req.tokens(i);
	}
	for (int i = 0; i < req.blockids_size(); i++) {
		const int64_t block_id = req.blockids(i);
	}
	GetHdfsBlockLocationsResponseProto res;
	// TODO get HDFS-specific metadata about blocks
	//res.add_volumeids() for each volume id
	//res.add_volumeindexes() for each volume index
	return Serialize(res);
}

std::string ClientDatanodeTranslator::shutdownDatanode(std::string input) {
	ShutdownDatanodeRequestProto req;
	req.ParseFromString(input);
	logMessage(req, "ShutdownDatanode ");
	const bool for_upgrade = req.forupgrade();
	ShutdownDatanodeResponseProto res;
	// TODO shut down the datanode. Response contains no fields
	return Serialize(res);
}

std::string ClientDatanodeTranslator::getDatanodeInfo(std::string input) {
	GetDatanodeInfoRequestProto req;
	req.ParseFromString(input);
	logMessage(req, "GetDatanodeInfo ");
	GetDatanodeInfoResponseProto res;
	// TODO get datanode info
	//res.set_localinfo();
	return Serialize(res);
}

/**
 * Temporary method to accept an OpReadBlockProto.
 */
std::string ClientDatanodeTranslator::_acceptReadBlock(std::string input) {
	OpReadBlockProto req;
	req.ParseFromString(input);
	logMessage(req, "ReadBlock ");
	std::string out;
	BlockOpResponseProto res;
	// TODO respond properly
	return Serialize(res);
}

/**
 * Serialize the message 'res' into out. If the serialization fails, then we must find out to handle it
 * If it succeeds, we simly return the serialized string.
 */
std::string ClientDatanodeTranslator::Serialize(google::protobuf::Message& res) {
	std::string out;
	if (!res.SerializeToString(&out)) {
		// TODO handle error
	}
	return out;
}

// ------------------------- CONFIG AND INITIALIZATION ------------------------

/**
 * Get an integer from the hdfs-defaults config
 */
int ClientDatanodeTranslator::getDefaultInt(std::string key) {
	return config.getInt(key);
}

/**
 * Initialize the rpc server
 */
void ClientDatanodeTranslator::InitServer() {
	LOG(INFO) << "Initializing datanode server...";
	RegisterClientRPCHandlers();
}

// ------------------------------------ RPC SERVER INTERACTIONS --------------------------

/**
 * Register our rpc handlers with the server
 */
void ClientDatanodeTranslator::RegisterClientRPCHandlers() {
	using namespace std::placeholders; // for `_1`

	// The reason for these binds is because it wants static functions, but we want to give it member functions
	// http://stackoverflow.com/questions/14189440/c-class-member-callback-simple-examples

	server.register_handler("getReplicaVisibleLength", std::bind(&ClientDatanodeTranslator::getReplicaVisibleLength, this, _1));
	server.register_handler("refreshNamenodes", std::bind(&ClientDatanodeTranslator::refreshNamenodes, this, _1));
	server.register_handler("deleteBlockPool", std::bind(&ClientDatanodeTranslator::deleteBlockPool, this, _1));
	server.register_handler("getBlockLocalPathInfo", std::bind(&ClientDatanodeTranslator::getBlockLocalPathInfo, this, _1));
	server.register_handler("getHdfsBlockLocations", std::bind(&ClientDatanodeTranslator::getHdfsBlockLocations, this, _1));
	server.register_handler("shutdownDatanode", std::bind(&ClientDatanodeTranslator::shutdownDatanode, this, _1));
	server.register_handler("getDatanodeInfo", std::bind(&ClientDatanodeTranslator::getDatanodeInfo, this, _1));
}

/**
 * Get the RPCServer this datanode uses to connect with clients
 */
RPCServer ClientDatanodeTranslator::getRPCServer() {
	return server;
}

/**
 * Get the port this datanode listens on
 */
int ClientDatanodeTranslator::getPort() {
	return port;
}

void ClientDatanodeTranslator::logMessage(google::protobuf::Message& req, std::string req_name) {
	LOG(INFO) << "Got message " << req_name << ": " << req.DebugString();
}

} //namespace
