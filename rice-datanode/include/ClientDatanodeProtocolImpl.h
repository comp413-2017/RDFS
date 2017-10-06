// Copyright 2017 Rice University, COMP 413 2017

#include <ConfigReader.h>
#include <google/protobuf/message.h>
#include <rpcserver.h>

#include <iostream>
#include <string>

#include "ClientDatanodeProtocol.pb.h"
#include "hdfs.pb.h"
#include "datatransfer.pb.h"

#pragma once

/**
 * The implementation of the rpc calls. 
 */
namespace client_datanode_translator {

class ClientDatanodeTranslator {
 public:
  explicit ClientDatanodeTranslator(int port);
  std::string getReplicaVisibleLength(std::string);
  std::string refreshNamenodes(std::string);
  std::string deleteBlockPool(std::string);
  std::string getBlockLocalPathInfo(std::string);
  std::string getHdfsBlockLocations(std::string);
  std::string shutdownDatanode(std::string);
  std::string getDatanodeInfo(std::string);
  std::string _acceptReadBlock(std::string);

  int getPort();
  RPCServer getRPCServer();

 private:
  std::string Serialize(google::protobuf::Message &);
  void InitServer();
  void RegisterClientRPCHandlers();
  void Config();
  void logMessage(google::protobuf::Message &req, std::string req_name);

  hadoop::hdfs::FsServerDefaultsProto server_defaults;
  int port;
  RPCServer server;

  config_reader::ConfigReader config;
};
}  // namespace client_datanode_translator
