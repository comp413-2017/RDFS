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
#include "HaServiceProtocolImpl.h"
#include "zk_nn_client.h"

namespace ha_service_translator {

// the .proto file implementation's namespace, used for messages
using namespace hadoop::common;

const std::string HaServiceTranslator::CLASS_NAME = ": **HaServiceTranslator** : ";

HaServiceTranslator::HaServiceTranslator(RPCServer* server_arg, zkclient::ZkNnClient& zk_arg, int port_arg)
	: server(server_arg), zk(zk_arg), port(port_arg) {
	RegisterServiceRPCHandlers();
	if (port == 5351){ // means 5351 is always the active node initially
		LOG(INFO) << CLASS_NAME << "NameNode running in ACTIVE state";
		state = ACTIVE;
	}
	else {
		LOG(INFO) << CLASS_NAME << "NameNode running in STANDBY state";
		state = STANDBY;
	}
}

std::string HaServiceTranslator::transitionToActive(std::string input) {
	TransitionToActiveRequestProto req;
	req.ParseFromString(input);
	logMessage(req, " Transition to Active ");
	LOG(INFO) << CLASS_NAME << "NameNode transitioning to ACTIVE state";
	state = ACTIVE;
	TransitionToActiveResponseProto res;
	return Serialize(res);
}

std::string HaServiceTranslator::transitionToStandby(std::string input) {
	TransitionToStandbyRequestProto req;
	req.ParseFromString(input);
	logMessage(req, " Transition to Standby ");
	LOG(INFO) << CLASS_NAME << "NameNode transitioning to STANDBY state";
	state = STANDBY;
	TransitionToStandbyResponseProto res;
	return Serialize(res);
}

std::string HaServiceTranslator::getServiceStatus(std::string input) {
	GetServiceStatusRequestProto req;
	req.ParseFromString(input);
	logMessage(req, " Get Serice Status ");
	GetServiceStatusResponseProto res;
	res.set_state(state);
	res.set_readytobecomeactive(true);
	return Serialize(res);
}

std::string HaServiceTranslator::monitorHealth(std::string input) {
	MonitorHealthRequestProto req;
	req.ParseFromString(input);
	logMessage(req, " Monitor health ");
	MonitorHealthResponseProto res;
	return Serialize(res);
}

// ----------------------- HANDLER HELPERS --------------------------------
/**
 * Serialize the message 'res' into out. If the serialization fails, then we must find out to handle it
 * If it succeeds, we simly return the serialized string.
 */
std::string HaServiceTranslator::Serialize(google::protobuf::Message& res) {
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
hadoop::common::RpcResponseHeaderProto HaServiceTranslator::GetErrorRPCHeader(std::string error_msg,
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

// ------------------------------------ RPC SERVER INTERACTIONS --------------------------

void HaServiceTranslator::RegisterServiceRPCHandlers() {
	using namespace std::placeholders; // for `_1`
	LOG(INFO) << CLASS_NAME << "Registering RPC Handlers";
	// The reason for these binds is because it wants static functions, but we want to give it member functions
    	// http://stackoverflow.com/questions/14189440/c-class-member-callback-simple-examples
	server->register_handler("transitionToActive", std::bind(&HaServiceTranslator::transitionToActive, this, _1));
	server->register_handler("transitionToStandby", std::bind(&HaServiceTranslator::transitionToStandby, this, _1));
	server->register_handler("getServiceStatus", std::bind(&HaServiceTranslator::getServiceStatus, this, _1));
	server->register_handler("monitorHealth", std::bind(&HaServiceTranslator::monitorHealth, this, _1));
}


// ------------------------------- HELPERS -----------------------------

void HaServiceTranslator::logMessage(google::protobuf::Message& req, std::string req_name) {
	LOG(INFO) << CLASS_NAME <<  "Got message " << req_name << ": " << req.DebugString();
}

HaServiceTranslator::~HaServiceTranslator() {
	// TODO handle being shut down
}
} //namespace
