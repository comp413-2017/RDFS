#include <iostream>
#include <string>
#include <thread>
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
#include <zkwrapper.h>
#include <ConfigReader.h>

#include "Leases.h"
#include "ClientNamenodeProtocolImpl.h"
#include "zk_nn_client.h"

/**
 * The implementation of the rpc calls.
 */
namespace client_namenode_translator {


// the .proto file implementation's namespace, used for messages
using namespace hadoop::hdfs;

const int ClientNamenodeTranslator::LEASE_CHECK_TIME = 60; // in seconds

ClientNamenodeTranslator::ClientNamenodeTranslator(int port_arg, zkclient::ZkNnClient& zk_arg)
	: port(port_arg), server(port), zk(zk_arg) {
	InitServer();
	std::thread(&ClientNamenodeTranslator::leaseCheck, this).detach();
	LOG(INFO) << "Created client namenode translator.";
}


// ----------------------- RPC HANDLERS ----------------------------

std::string ClientNamenodeTranslator::getFileInfo(std::string input) {
	GetFileInfoRequestProto req;
	req.ParseFromString(input);
	logMessage(req, "GetFileInfo ");
	GetFileInfoResponseProto res;
	zk.get_info(req, res);
	std::cout << "hello " << std::endl;
	logMessage(res, "GetFileInfo response ");
	return Serialize(res);
}

std::string ClientNamenodeTranslator::mkdir(std::string input) {
	MkdirsRequestProto req;
	req.ParseFromString(input);
	logMessage(req, "Mkdir ");
	MkdirsResponseProto res;
	// TODO for now, just say the mkdir command failed
	zk.mkdir(req, res);
	logMessage(res, "Mkdir response ");
	return Serialize(res);
}

std::string ClientNamenodeTranslator::destroy(std::string input) {
	DeleteRequestProto req;
	req.ParseFromString(input);
	logMessage(req, "Delete ");
	const std::string& src = req.src();
	const bool recursive = req.recursive();
	std::string out;
	DeleteResponseProto res;
	// TODO for now, just say the delete command failed
	res.set_result(false);
	return Serialize(res);
}

std::string ClientNamenodeTranslator::create(std::string input) {
	CreateRequestProto req;
	req.ParseFromString(input);
	logMessage(req, "Create ");
	CreateResponseProto res = CreateResponseProto::default_instance();
	if (zk.create_file(req, res))
		lease_manager.addLease(req.clientname(), req.src());
	logMessage(res, "Create response ");
    	return Serialize(res);
}


std::string ClientNamenodeTranslator::getBlockLocations(std::string input) {
	GetBlockLocationsRequestProto req;
	req.ParseFromString(input);
	logMessage(req, "GetBlockLocations ");
	GetBlockLocationsResponseProto res;
	zk.get_block_locations(req, res);
	return Serialize(res);
}

std::string ClientNamenodeTranslator::getServerDefaults(std::string input) {
	GetServerDefaultsRequestProto req;
	req.ParseFromString(input);
	logMessage(req, "GetServerDefaults ");
	std::string out;
	GetServerDefaultsResponseProto res;
	FsServerDefaultsProto def;
	// read all this config info
	def.set_blocksize(getDefaultInt("dfs.blocksize"));
	def.set_bytesperchecksum(getDefaultInt("dfs.bytes-per-checksum"));
	def.set_writepacketsize(getDefaultInt("dfs.client-write-packet-size"));
	def.set_replication(getDefaultInt("dfs.replication"));
	def.set_filebuffersize(getDefaultInt("dfs.stream-buffer-size"));
	def.set_encryptdatatransfer(getDefaultInt("dfs.encrypt.data.transfer"));
	// TODO ChecksumTypeProto (optional)	
	res.set_allocated_serverdefaults(&def);
	return Serialize(res);
}

std::string ClientNamenodeTranslator::renewLease(std::string input) {
	RenewLeaseRequestProto req;
	req.ParseFromString(input);
	logMessage(req, "RenewLease ");
	const std::string& clientname = req.clientname();
	// renew the lease for all files associated with this client 
	lease_manager.renewLeases(clientname);
	RenewLeaseResponseProto res;
	return Serialize(res);
}

std::string ClientNamenodeTranslator::complete(std::string input) {
	CompleteRequestProto req;
	req.ParseFromString(input);
	logMessage(req, "Complete ");
	CompleteResponseProto res;
	// TODO some optional fields need to be read
	zk.complete(req, res); 
	// remove the lease from this file  
	bool succ = lease_manager.removeLease(req.clientname(), req.src());
	if (!succ) {
		LOG(ERROR) << "A client tried to close a file which is not theirs";
	}
	// TODO close the file (communicate with zookeeper) and do any recovery necessary
	// for now, we claim to succeed.
	return Serialize(res);
}

/**
 * The actual block replication is not expected to be performed during  
 * this method call. The blocks will be populated or removed in the 
 * background as the result of the routine block maintenance procedures.
 */
std::string ClientNamenodeTranslator::setReplication(std::string input) {
	SetReplicationRequestProto req;
	req.ParseFromString(input);
	logMessage(req, "SetReplication ");
	const std::string& src = req.src();
	google::protobuf::uint32 replication = req.replication();
	// TODO verify file exists, set its replication, for now, we fail 
	SetReplicationResponseProto res;
	res.set_result(false);
	return Serialize(res);
}

/**
 * The client can give up on a block by calling abandonBlock().
 * The client can then either obtain a new block, or complete or
 * abandon the file. Any partial writes to the block will be discarded.
 */
std::string ClientNamenodeTranslator::abandonBlock(std::string input) {
	AbandonBlockRequestProto req;
	req.ParseFromString(input);
	logMessage(req, "AbandonBlock ");
	const ExtendedBlockProto& blockProto = req.b();
	const std::string& src = req.src();
	const std::string& holder = req.src(); // TODO who is the holder??
	// TODO some optional fields
	// TODO tell zookeeper to get rid of the last block (should not need to 
	// (talk to datanode as far as i am aware)

	AbandonBlockResponseProto res; 
	return Serialize(res);	
}

// ----------------------- COMMANDS WE DO NOT SUPPORT ------------------

std::string ClientNamenodeTranslator::rename(std::string input) {
	RenameResponseProto res;
	res.set_result(false);
	return Serialize(res);
}

std::string ClientNamenodeTranslator::rename2(std::string input) {
	Rename2RequestProto req;
	req.ParseFromString(input);
	logMessage(req, "Rename2 ");
	Rename2ResponseProto res;
	return Serialize(res);	
}

std::string ClientNamenodeTranslator::append(std::string input) {
	AppendRequestProto req;
	req.ParseFromString(input);
	logMessage(req, "Append ");
	AppendResponseProto res;
	return Serialize(res);
}

/**
 * This is effectively an append, so we do not support it 
 */
std::string ClientNamenodeTranslator::concat(std::string input) {
	ConcatRequestProto req;
	req.ParseFromString(input);
	logMessage(req, "Concat ");
	ConcatResponseProto res;
	return Serialize(res);	
}

/**
 * TODO we might support this in the future!
 */ 
std::string ClientNamenodeTranslator::setPermission(std::string input) {
	SetPermissionResponseProto res;
	return Serialize(res);
}

/**
 * While we expect clients to renew their lease, we should never allow
 * a client to "recover" a lease, since we only allow a write-once system
 */ 
std::string ClientNamenodeTranslator::recoverLease(std::string input) {
	RecoverLeaseRequestProto req;
	req.ParseFromString(input);
	logMessage(req, "RecoverLease ");
	RecoverLeaseResponseProto res;
	// just tell the client they could not recover the lease, so they won't try and write
	res.set_result(false);
	return Serialize(res);
}


// ----------------------- HANDLER HELPERS --------------------------------

/**
 * Serialize the message 'res' into out. If the serialization fails, then we must find out to handle it
 * If it succeeds, we simly return the serialized string. 
 */
std::string ClientNamenodeTranslator::Serialize(google::protobuf::Message& res) {
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
int ClientNamenodeTranslator::getDefaultInt(std::string key) {
	return config.getInt(key);
}

/**
 * Initialize the rpc server
 */
void ClientNamenodeTranslator::InitServer() {
	LOG(INFO) << "Initializing namenode server...";
	RegisterClientRPCHandlers();
}

// ------------------------------------ RPC SERVER INTERACTIONS --------------------------

/**
 * Register our rpc handlers with the server
 */
void ClientNamenodeTranslator::RegisterClientRPCHandlers() {
	using namespace std::placeholders; // for `_1`

	// The reason for these binds is because it wants static functions, but we want to give it member functions
    // http://stackoverflow.com/questions/14189440/c-class-member-callback-simple-examples
	server.register_handler("getFileInfo", std::bind(&ClientNamenodeTranslator::getFileInfo, this, _1));
	server.register_handler("mkdirs", std::bind(&ClientNamenodeTranslator::mkdir, this, _1));
	server.register_handler("append", std::bind(&ClientNamenodeTranslator::append, this, _1));
	server.register_handler("destroy", std::bind(&ClientNamenodeTranslator::destroy, this, _1));
	server.register_handler("create", std::bind(&ClientNamenodeTranslator::create, this, _1));
	server.register_handler("setReplication", std::bind(&ClientNamenodeTranslator::setReplication, this, _1));
	server.register_handler("abandonBlock", std::bind(&ClientNamenodeTranslator::abandonBlock, this, _1));
	server.register_handler("renewLease", std::bind(&ClientNamenodeTranslator::renewLease, this, _1));
	server.register_handler("getServerDefaults", std::bind(&ClientNamenodeTranslator::getServerDefaults, this, _1));
	server.register_handler("complete", std::bind(&ClientNamenodeTranslator::complete, this, _1));
	server.register_handler("getBlockLocations", std::bind(&ClientNamenodeTranslator::getBlockLocations, this, _1));

	// register handlers for unsupported calls
	server.register_handler("rename", std::bind(&ClientNamenodeTranslator::rename, this, _1));
	server.register_handler("rename2", std::bind(&ClientNamenodeTranslator::rename2, this, _1));
	server.register_handler("append", std::bind(&ClientNamenodeTranslator::append, this, _1));
	server.register_handler("recoverLease", std::bind(&ClientNamenodeTranslator::recoverLease, this, _1));
	server.register_handler("concat", std::bind(&ClientNamenodeTranslator::concat, this, _1));
}

/**
 * Get the RPCServer this namenode uses to connect with clients
 */ 
RPCServer ClientNamenodeTranslator::getRPCServer() {
	return server; 
} 

/**
 * Get the port this namenode listens on
 */
int ClientNamenodeTranslator::getPort() {
	return port;
}

// ------------------------------- LEASES ----------------------------

void ClientNamenodeTranslator::leaseCheck() {
	LOG(INFO) << "Lease manager check initialized";
	for (;;) {
		sleep(LEASE_CHECK_TIME); // only check every 60 seconds
		std::vector<std::string> expiredFiles = lease_manager.checkLeases(LEASE_CHECK_TIME);
		for (std::string file : expiredFiles) {
			// TODO close file on behalf of client, and standardize the last block written
		}
	}
}

// ------------------------------- HELPERS -----------------------------

void ClientNamenodeTranslator::logMessage(google::protobuf::Message& req, std::string req_name) {
	LOG(INFO) << "Got message " << req_name << ": " << req.DebugString();
}

ClientNamenodeTranslator::~ClientNamenodeTranslator() {
	// TODO handle being shut down
}
} //namespace
