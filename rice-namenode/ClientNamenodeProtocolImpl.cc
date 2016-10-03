#include <iostream>
#include <string>
#include <google/protobuf/arena.h>
#include <google/protobuf/arenastring.h>
#include <google/protobuf/generated_message_util.h>
#include <google/protobuf/metadata.h>
#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/extension_set.h>
#include <google/protobuf/generated_enum_reflection.h>
#include <google/protobuf/unknown_field_set.h>

#include <rpcserver.h>
#include <pugixml.hpp>

#include "ClientNamenodeProtocolImpl.h"

/**
 * The implementation of the rpc calls. 
 */
namespace client_namenode_translator {

// the .proto file implementation's namespace, used for messages
using namespace hadoop::hdfs;

// static string info
const char* ClientNamenodeTranslator::HDFS_DEFAULTS_CONFIG = "hdfs-default.xml";

// TODO - this will probably take some zookeeper object
ClientNamenodeTranslator::ClientNamenodeTranslator(int port_arg)
	: port(port_arg), server(port) {
	InitServer();
	Config();
	std::cout << "Created client namenode translator." << std::endl;
}

std::string ClientNamenodeTranslator::getFileInfo(std::string input) {
	std::cout << "Got getFileInfo request with input " << input << std::endl;
	GetFileInfoRequestProto req;
	req.ParseFromString(input);
	const std::string& src = req.src();
	// from here, we would ask zoo-keeper something, we should check
	// the response, and either return the response or return some 
	// void response...for now we will just return			
	std::string out; 
	GetFileInfoResponseProto res;
	return Serialize(&out, res);
}

std::string ClientNamenodeTranslator::mkdir(std::string input) {
	std::cout << "Got mkdir request with input " << input << std::endl;
	MkdirsRequestProto req;
	req.ParseFromString(input);
	const std::string& src = req.src();
	const hadoop::hdfs::FsPermissionProto& permission_msg = req.masked();
	bool create_parent = req.createparent();
	std::string out;
	MkdirsResponseProto res;
	// TODO for now, just say the mkdir command failed
	res.set_result(false);
	return Serialize(&out, res);
}

std::string ClientNamenodeTranslator::append(std::string input) {
	std::cout << "Got append request with input " << input << std::endl;
	AppendRequestProto req;
	req.ParseFromString(input);
	const std::string& src = req.src();
	const std::string& clientName = req.clientname();
	std::string out;
	AppendResponseProto res;
	// TODO We don't support this operation, so we need to return some
	// kind of failure status. I've looked around and I'm not sure 
	// how to do this since this message only contains an optional
	// LocatedBlockProto. No LocatedBlockProto might be failure
	return Serialize(&out, res);
}

// TODO delete is a keyword in C++. Come up with a better replacement name than
// deleteCmd
std::string ClientNamenodeTranslator::deleteCmd(std::string input) {
	std::cout << "Got delete request with input " << input << std::endl;
	DeleteRequestProto req;
	req.ParseFromString(input);
	const std::string& src = req.src();
	const bool recursive = req.recursive();
	std::string out;
	DeleteResponseProto res;
	// TODO for now, just say the delete command failed
	res.set_result(false);
	return Serialize(&out, res);
}

std::string ClientNamenodeTranslator::create(std::string input) {
	std::cout << "Got create request with input " << input << std::endl;
	CreateRequestProto req;
	req.ParseFromString(input);
	const std::string& src = req.src();
	const hadoop::hdfs::FsPermissionProto& masked = req.masked();
	std::string out;
	CreateResponseProto res;
	// TODO for now, just say the create command failed. Not entirely sure
	// how to do that, but I think you just don't include an
	// HDFSFileStatusProto
	return Serialize(&out, res);
}


std::string ClientNamenodeTranslator::getBlockLocations(std::string input) {
	std::cout << "Got create request with input " << input << std::endl;
	GetBlockLocationsRequestProto req;
	req.ParseFromString(input);
	const std::string& src = req.src();
	google::protobuf::uint64 offset = req.offset();
	google::protobuf::uint64 length = req.offset();
	std::string out;
	GetBlockLocationsResponseProto res;
	// TODO for now, just say the getBlockLocations command failed. Not entirely sure
	// how to do that, but I think you just don't include a
	// LocatedBlocksProto
	return Serialize(&out, res);
}

std::string ClientNamenodeTranslator::getServerDefaults(std::string input) {
	std::cout << "Got server defaults request " << input << std::endl;
	GetServerDefaultsRequestProto req;
	req.ParseFromString(input);
    std::string out;
	GetServerDefaultsResponseProto res;
    return Serialize(&out, res);
}

/**
 * Serialize the message 'res' into out. If the serialization fails, then we must find out to handle it
 * If it succeeds, we simly return the serialized string. 
 */
std::string ClientNamenodeTranslator::Serialize(std::string* out, google::protobuf::Message& res) {
	if (!res.SerializeToString(out)) {
		// TODO handle error
	}
	return *out;
}

/**
 * Set the configuration info for the namenode
 */
void ClientNamenodeTranslator::Config() {
	// Read the hdfs-defaults xml file 
	{
		using namespace pugi;
		xml_document doc;
		xml_parse_result result = doc.load_file(HDFS_DEFAULTS_CONFIG);
		if (!result) {
		    std::cout << "XML [" << HDFS_DEFAULTS_CONFIG << "] parsed with errors, attr value: [" << doc.child("node").attribute("attr").value() << "]\n";
    		std::cout << "Error description: " << result.description() << "\n";
		}
			
		xml_node properties = doc.child("configuration");
		for (xml_node child : properties.children()) {
			// the name and value nodes in the xml 
			xml_node name = child.first_child();
            xml_node value = name.next_sibling();	
			const char* name_str = name.first_child().text().get();
			// TODO best way to do this? there are a lot of cases 	
			if (strcmp(name_str, "dfs.namenode.fs-limits.min-block-size") == 0) {
				xml_node value = name.next_sibling();
				int min_block_size = value.first_child().text().as_int();;
				// TODO for example, here, we would add this field to our member FsServerDefault info
			}
		}
        std::cout << "Configured namenode (but not really!)" << std::endl;
	}

	// TODO any other configs that we need to read? 	
}

/**
 * Initialize the rpc server
 */
void ClientNamenodeTranslator::InitServer() {
	RegisterClientRPCHandlers();
}

/**
 * Register our rpc handlers with the server
 */
void ClientNamenodeTranslator::RegisterClientRPCHandlers() {
    using namespace std::placeholders; // for `_1`

	// The reason for these binds is because it wants static functions, but we want to give it member functions
    // http://stackoverflow.com/questions/14189440/c-class-member-callback-simple-examples

	server.register_handler("getFileInfo", std::bind(&ClientNamenodeTranslator::getFileInfo, this, _1));
	server.register_handler("mkdir", std::bind(&ClientNamenodeTranslator::mkdir, this, _1));
	server.register_handler("append", std::bind(&ClientNamenodeTranslator::append, this, _1));
	server.register_handler("deleteCmd", std::bind(&ClientNamenodeTranslator::deleteCmd, this, _1));
	server.register_handler("create", std::bind(&ClientNamenodeTranslator::create, this, _1));
	server.register_handler("getBlockLocations", std::bind(&ClientNamenodeTranslator::getBlockLocations, this, _1));
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

} //namespace
