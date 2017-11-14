// Copyright 2017 Rice University, COMP 413 2017

#include <google/protobuf/message.h>
#include <rpcserver.h>
#include <zkwrapper.h>
#include <string>
#include <iostream>
#include "ClientNamenodeProtocol.pb.h"
#include "hdfs.pb.h"
#include <RpcHeader.pb.h>
#include <ConfigReader.h>
#include "zk_nn_client.h"
#include "DaemonFactory.h"

#pragma once

/**
 * The implementation of the rpc calls. 
 */
namespace client_namenode_translator {

// the .proto file implementation's namespace, used for messages
using hadoop::hdfs::FsServerDefaultsProto;

/**
 * The translator receives the rpc parameters from rpcserver. It then processes
 * the message and does whatever is necessary, returing a serializes protobuff.
 *
 * It communicates with zookeeper to construct the namespace and communicate with datanode 
 */
class ClientNamenodeTranslator {
 public:
  ClientNamenodeTranslator(int port, zkclient::ZkNnClient *zk_arg);
  ~ClientNamenodeTranslator();

  // RPC calls which we support. Each take a string which comes form
  // the rpc call, and it is then deserialized into their proto msg
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
  std::string getListing(std::string input);
  std::string getEZForPath(std::string input);
  std::string setOwner(std::string input);
  std::string getContentSummary(std::string input);
  std::string modifyAclEntries(std::string input);
  std::string setPermission(std::string);

  // RPC calls which are not supported
  std::string rename(std::string);
  std::string rename2(std::string);
  std::string append(std::string);

  std::string recoverLease(std::string);
  std::string concat(std::string);

  // lease manager interactions
  std::string abandonBlock(std::string input);

  // Additional methods for file append process - Anthony, Marc
  std::string fsync(std::string input);
  std::string finalizeUpgrade(std::string input);

  int getPort();
  RPCServer getRPCServer();

 private:
  std::string Serialize(google::protobuf::Message &);
  /**
   * Construct the RPC server
   */
  void InitServer();

  /**
   * Register all the methods with the server that handle RPC calls
   */
  void RegisterClientRPCHandlers();

  /**
   * Construct the full zookeeper path from a hadoop path
   */
  std::string ZookeeperPath(const std::string &hadoopPath);

  // server defaults as read from the config
  FsServerDefaultsProto server_defaults;
  // port which our rpc server is using
  int port;
  // our rpc server
  RPCServer server;
  // client to communicate with zookeeper
  zkclient::ZkNnClient *zk;
  // used to read from our config files
  config_reader::ConfigReader config;

  /**
   * Log incoming messages "req" for rpc call "req_name"
   */
  void logMessage(google::protobuf::Message *req, std::string req_name);

  /**
   * Get an int from the config file for our defaults
   */
  int getDefaultInt(std::string);

  /**
   * Get an rpc header proto given an error message and exception classname
   */
  hadoop::common::RpcResponseHeaderProto GetErrorRPCHeader(
      std::string error_msg,
      std::string exception_classname);

  static const std::string CLASS_NAME;
};  // class
}  // namespace client_namenode_translator
