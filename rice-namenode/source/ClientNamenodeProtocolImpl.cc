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
#include <RpcHeader.pb.h>

#include <easylogging++.h>
#include <rpcserver.h>
#include <zkwrapper.h>
#include <ConfigReader.h>

#include "Leases.h"
#include "ClientNamenodeProtocolImpl.h"
#include "zk_nn_client.h"

/**
 * The implementation of the rpc calls.
 *
 * Take a look at rpcserver.cc to see where the calls to these functions are
 * coming from - generally, it will be in the block following the "if(iter !=
 * dispatch_table.end()) {" line, which is more or less just checking that the
 * requested command has a function in this file.
 *
 */
namespace client_namenode_translator {


// the .proto file implementation's namespace, used for messages
using namespace hadoop::hdfs;

const int ClientNamenodeTranslator::LEASE_CHECK_TIME = 60; // in seconds

const std::string ClientNamenodeTranslator::CLASS_NAME = ": **ClientNamenodeTranslator** : ";


ClientNamenodeTranslator::ClientNamenodeTranslator(int port_arg, zkclient::ZkNnClient& zk_arg)
	: port(port_arg), server(port), zk(zk_arg) {
	InitServer();
	std::thread(&ClientNamenodeTranslator::leaseCheck, this).detach();
	LOG(INFO) << CLASS_NAME <<  "Created client namenode translator.";
}


// ----------------------- RPC HANDLERS ----------------------------

std::string ClientNamenodeTranslator::getFileInfo(std::string input) {
	GetFileInfoRequestProto req;
	req.ParseFromString(input);
	logMessage(req, "GetFileInfo ");
	GetFileInfoResponseProto res;
	zk.get_info(req, res);
	logMessage(res, "GetFileInfo response ");
	return Serialize(res);
}

std::string ClientNamenodeTranslator::mkdir(std::string input) {
	MkdirsRequestProto req;
	req.ParseFromString(input);
	logMessage(req, "Mkdir ");
	MkdirsResponseProto res;
	zk.mkdir(req, res);
	return Serialize(res);
}

std::string ClientNamenodeTranslator::destroy(std::string input) {
	DeleteRequestProto req;
	req.ParseFromString(input);
	logMessage(req, "Delete ");
	const std::string& src = req.src();
	const bool recursive = req.recursive();
	DeleteResponseProto res;
	zk.destroy(req, res);
	return Serialize(res);
}

std::string ClientNamenodeTranslator::create(std::string input) {
	CreateRequestProto req;
	req.ParseFromString(input);
	logMessage(req, "Create ");
	CreateResponseProto res;
	if (zk.create_file(req, res))
		lease_manager.addLease(req.clientname(), req.src());
	else {
		throw GetErrorRPCHeader("Could not create file", "IOException");
	}
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
	logMessage(req, "GetServerDefaults");
	GetServerDefaultsResponseProto res;
	FsServerDefaultsProto* def = res.mutable_serverdefaults();
	// read all this config info
	def->set_blocksize(getDefaultInt("dfs.blocksize"));
	def->set_bytesperchecksum(getDefaultInt("dfs.bytes-per-checksum"));
	def->set_writepacketsize(getDefaultInt("dfs.client-write-packet-size"));
	def->set_replication(getDefaultInt("dfs.replication"));
	def->set_filebuffersize(getDefaultInt("dfs.stream-buffer-size"));
	def->set_encryptdatatransfer(getDefaultInt("dfs.encrypt.data.transfer"));
	// TODO ChecksumTypeProto (optional)
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
	bool succ = lease_manager.removeLease(req.clientname(), req.src());
	if (!succ) {
		LOG(ERROR) << CLASS_NAME <<  "A client tried to close a file which is not theirs";
		res.set_result(false);
	} else {
		zk.complete(req, res);
	}
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

std::string ClientNamenodeTranslator::addBlock(std::string input) {
	AddBlockRequestProto req;
	AddBlockResponseProto res;
	req.ParseFromString(input);
	logMessage(req, "AddBlock ");
	if(zk.add_block(req, res)){
		return Serialize(res);}
	else{
		throw GetErrorRPCHeader("Could not add block", "");
	}
}

std::string ClientNamenodeTranslator::rename(std::string input) {
	RenameRequestProto req;
	RenameResponseProto res;
	req.ParseFromString(input);
	logMessage(req, "Rename ");
	zk.rename(req, res);
	return Serialize(res);
}

std::string ClientNamenodeTranslator::setPermission(std::string input) {
		SetPermissionResponseProto res;
			return Serialize(res);
}
// ----------------------- COMMANDS WE DO NOT SUPPORT ------------------
/**
 * When asked to do an unsupported command, we'll be returning a
 * method-not-found proto.  The code in question is very similar to
 * GetErrorRPCHeader, but will actually occur further up - see rpcserver.cc's
 * method handle_rpc. Whenever (iter != dispatch_table.end()) is false, it
 * basically means that we couldn't find a corresponding method in this file
 * here. 
 *
 * As such, it will go ahead and create the error header and send it back
 * along, without ever having to call any methods in this file. So there is
 * no need to ever worry about methods we just flat out don't support in this 
 * file.
 *
 * That being said, the following is a short list of some common commands we don't
 * support, and our reasons for not supporting them:
 *
 * 1. setReplication:
 * The actual block replication is not expected to be performed during  
 * this method call. The blocks will be populated or removed in the 
 * background as the result of the routine block maintenance procedures.
 * Basically, cannot set replication to something new.
 *
 * 2. append: 
 * Appends are not supported
 *
 * 3. concat:
 * Effectively an append, so we don't support it
 *
 * 4. recoverLease:
 * While we expect clients to renew their lease, we should never allow
 * a client to "recover" a lease, since we only allow a write-once system
 * As such, we cannot recover leases.
 *
 * 5. setPermission:
 * TODO - might support this later!
 *
 */

// ------------------------------ TODO ------------------------------------



// TODO what is this? It originally was inside the "commands not supported"
// block, but it seems to be doing something?
std::string ClientNamenodeTranslator::rename2(std::string input) {
	Rename2RequestProto req;
	req.ParseFromString(input);
	logMessage(req, "Rename2 ");
	Rename2ResponseProto res;
	return Serialize(res);
}



// ----------------------- HANDLER HELPERS --------------------------------

/**
 * Serialize the message 'res' into out. If the serialization fails, then we must find out to handle it
 * If it succeeds, we simly return the serialized string. 
 */
std::string ClientNamenodeTranslator::Serialize(google::protobuf::Message& res) {
	std::string out;
	logMessage(res, "Responding with ");
	if (!res.SerializeToString(&out)) {
		// TODO handle error
	}
	return out;
}

/**
 * Get an error rpc header given an error msg and exception classname
 *
 * (Note - this method shouldn't be used in the case that we choose not to
 * support a command being called. Those cases should be handled back in
 * rpcserver.cc, which will be using a very similar - but different - function)
 */
hadoop::common::RpcResponseHeaderProto ClientNamenodeTranslator::GetErrorRPCHeader(std::string error_msg,
		std::string exception_classname) {
	hadoop::common::RpcResponseHeaderProto response_header;
	response_header.set_status(hadoop::common::RpcResponseHeaderProto_RpcStatusProto_ERROR);
	response_header.set_errormsg(error_msg);
	response_header.set_exceptionclassname(exception_classname);
    //TODO - since this method is now only being used for failed handlers, this line seems
    //to be incorrect. As far as I can tell, only create uses this method now.
	response_header.set_errordetail(hadoop::common::RpcResponseHeaderProto_RpcErrorCodeProto_ERROR_APPLICATION);
	return response_header;
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
	LOG(INFO) << CLASS_NAME <<  "Initializing namenode server...";
	RegisterClientRPCHandlers();
}

// ------------------------------------ RPC SERVER INTERACTIONS --------------------------

/**
 * Register our rpc handlers with the server (rpcserver cals corresponding
 * handler methods for any requested commands based on request_header.methodname())
 *
 * Note - do not make handlers for unsupported commands! Whenever a command
 * doesn't have an entry in this map, rpcserver.cc will just send a response
 * header with error detail "ERROR_NO_SUCH_METHOD." Any handlers registered
 * here should be for supported commands.
 */
void ClientNamenodeTranslator::RegisterClientRPCHandlers() {
	using namespace std::placeholders; // for `_1`

	// The reason for these binds is because it wants static functions, but we want to give it member functions
    	// http://stackoverflow.com/questions/14189440/c-class-member-callback-simple-examples
	server.register_handler("getFileInfo", std::bind(&ClientNamenodeTranslator::getFileInfo, this, _1));
	server.register_handler("mkdirs", std::bind(&ClientNamenodeTranslator::mkdir, this, _1));
	server.register_handler("delete", std::bind(&ClientNamenodeTranslator::destroy, this, _1));
	server.register_handler("create", std::bind(&ClientNamenodeTranslator::create, this, _1));
	server.register_handler("abandonBlock", std::bind(&ClientNamenodeTranslator::abandonBlock, this, _1));
	server.register_handler("renewLease", std::bind(&ClientNamenodeTranslator::renewLease, this, _1));
	server.register_handler("getServerDefaults", std::bind(&ClientNamenodeTranslator::getServerDefaults, this, _1));
	server.register_handler("complete", std::bind(&ClientNamenodeTranslator::complete, this, _1));
	server.register_handler("getBlockLocations", std::bind(&ClientNamenodeTranslator::getBlockLocations, this, _1));
	server.register_handler("addBlock", std::bind(&ClientNamenodeTranslator::addBlock, this, _1));

	//TODO - what is this function for? Do we still need it??
	server.register_handler("rename2", std::bind(&ClientNamenodeTranslator::rename2, this, _1));
	server.register_handler("rename", std::bind(&ClientNamenodeTranslator::rename, this, _1));
	//server.register_handler("append", std::bind(&ClientNamenodeTranslator::append, this, _1));
	//server.register_handler("recoverLease", std::bind(&ClientNamenodeTranslator::recoverLease, this, _1));
	//server.register_handler("concat", std::bind(&ClientNamenodeTranslator::concat, this, _1));
	server.register_handler("setPermission", std::bind(&ClientNamenodeTranslator::setPermission, this, _1));

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
	LOG(INFO) << CLASS_NAME <<  "Lease manager check initialized";
	for (;;) {
		sleep(LEASE_CHECK_TIME); // only check every 60 seconds
		std::vector<std::pair<std::string, std::string>> expiredFiles = lease_manager.checkLeases(LEASE_CHECK_TIME);
		for (auto client_file : expiredFiles) {
			std::string client = client_file.first;
			std::string file = client_file.second;
			CompleteRequestProto req;
			req.set_src(file);
			req.set_clientname(client);
			complete(Serialize(req));
		}
	}
}

// ------------------------------- HELPERS -----------------------------

void ClientNamenodeTranslator::logMessage(google::protobuf::Message& req, std::string req_name) {
	LOG(INFO) << CLASS_NAME <<  "Got message " << req_name << ": " << req.DebugString();
}

ClientNamenodeTranslator::~ClientNamenodeTranslator() {
	// TODO handle being shut down
}
} //namespace
