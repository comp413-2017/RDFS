// Copyright 2017 Rice University, COMP 413 2017

#include <google/protobuf/message.h>
#include <rpcserver.h>
#include <zkwrapper.h>
#include <string>
#include <iostream>
#include "HAServiceProtocol.pb.h"
#include "hdfs.pb.h"
#include <RpcHeader.pb.h>
#include <ConfigReader.h>
#include "zk_nn_client.h"

#pragma once
/**
 * The implementation of the rpc calls.
 */
namespace ha_service_translator {

// the .proto file implementation's namespace, used for messages
using hadoop::common::RpcResponseHeaderProto;
using hadoop::common::HAServiceStateProto;

/**
 * The translator receives the rpc parameters from rpcserver. It then processes
 * the message and does whatever is necessary, returing a serializes protobuff.
 *
 * It communicates with zookeeper to construct the namespace and communicate with datanode
 */

class HaServiceTranslator {
 public:
  HaServiceTranslator(
      RPCServer *server_arg,
      zkclient::ZkNnClient &zk_arg,
      int port_arg);

  // RPC calls which we support. Each take a string which comes form
  // the rpc call, and it is then deserialized into their proto msg
  std::string transitionToActive(std::string);
  std::string transitionToStandby(std::string);
  std::string getServiceStatus(std::string);
  std::string monitorHealth(std::string input);

 private:
  std::string Serialize(google::protobuf::Message &);

  /**
   * Register all the methods with the server that handle RPC calls
   */
  void RegisterServiceRPCHandlers();

  // server defaults as read from the config
  // FsServerDefaultsProto server_defaults;
  // port which our rpc server is using
  int port;
  // our rpc server
  RPCServer *server;
  // client to communicate with zookeeper
  zkclient::ZkNnClient &zk;
  HAServiceStateProto state;

  /**
   * Log incoming messages "req" for rpc call "req_name"
   */
  void logMessage(google::protobuf::Message &req, std::string req_name);

  /**
   * Get an rpc header proto given an error message and exception classname
   */
  RpcResponseHeaderProto GetErrorRPCHeader(
      std::string error_msg,
      std::string exception_classname);

  static const std::string CLASS_NAME;
};  // class
}  // namespace ha_service_translator
