// Copyright 2017 Rice University, COMP 413 2017

#include "ClientDatanodeProtocolImpl.h"

#include <easylogging++.h>
#include <google/protobuf/arena.h>
#include <google/protobuf/arenastring.h>
#include <google/protobuf/extension_set.h>
#include <google/protobuf/generated_enum_reflection.h>
#include <google/protobuf/generated_message_util.h>
#include <google/protobuf/message.h>
#include <google/protobuf/metadata.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/unknown_field_set.h>
#include <rpcserver.h>
#include <unistd.h>

#include <iostream>
#include <string>

using hadoop::hdfs::ExtendedBlockProto;
using hadoop::hdfs::DeleteBlockPoolRequestProto;
using hadoop::hdfs::DeleteBlockPoolResponseProto;
using hadoop::hdfs::GetBlockLocalPathInfoRequestProto;
using hadoop::hdfs::GetBlockLocalPathInfoResponseProto;
using hadoop::hdfs::GetDatanodeInfoRequestProto;
using hadoop::hdfs::GetDatanodeInfoResponseProto;
using hadoop::hdfs::GetHdfsBlockLocationsRequestProto;
using hadoop::hdfs::GetHdfsBlockLocationsResponseProto;
using hadoop::hdfs::GetReplicaVisibleLengthRequestProto;
using hadoop::hdfs::GetReplicaVisibleLengthResponseProto;
using hadoop::hdfs::RefreshNamenodesRequestProto;
using hadoop::hdfs::RefreshNamenodesResponseProto;
using hadoop::hdfs::ShutdownDatanodeRequestProto;
using hadoop::hdfs::ShutdownDatanodeResponseProto;

/**
 * The implementation of the rpc calls. 
 */
namespace client_datanode_translator {

// config
std::map<std::string, std::string> config;

// TODO(anyone): this will probably take some zookeeper object
ClientDatanodeTranslator::ClientDatanodeTranslator(int port_arg)
    : port(port_arg), server(port) {
  InitServer();
  LOG(INFO) << "Created client datanode translator.";
}

std::string ClientDatanodeTranslator::getReplicaVisibleLength(
  std::string input) {
  GetReplicaVisibleLengthRequestProto req;
  req.ParseFromString(input);
  logMessage(req, "GetReplicaVisibleLength ");
  const ExtendedBlockProto &block = req.block();
  GetReplicaVisibleLengthResponseProto res;
  // TODO(anyone): get the visible length of the block and set it the response
  return Serialize(res);
}

std::string ClientDatanodeTranslator::refreshNamenodes(std::string input) {
  RefreshNamenodesRequestProto req;
  req.ParseFromString(input);
  logMessage(req, "RefreshNamenodes ");
  RefreshNamenodesResponseProto res;
  // TODO(anyone): refresh the namenodes. Response contains no fields
  return Serialize(res);
}

std::string ClientDatanodeTranslator::deleteBlockPool(std::string input) {
  DeleteBlockPoolRequestProto req;
  req.ParseFromString(input);
  logMessage(req, "DeleteBlockPool ");
  const std::string &block_pool = req.blockpool();
  const bool force = req.force();
  DeleteBlockPoolResponseProto res;
  // TODO(anyone): delete the block pool. Response contains no fields
  return Serialize(res);
}

std::string ClientDatanodeTranslator::getBlockLocalPathInfo(std::string input) {
  GetBlockLocalPathInfoRequestProto req;
  req.ParseFromString(input);
  logMessage(req, "GetBlockLocalPathInfo ");
  const ExtendedBlockProto &block = req.block();
  const hadoop::common::TokenProto &token = req.token();
  GetBlockLocalPathInfoResponseProto res;
  // TODO(anyone): get local path info for block
  return Serialize(res);
}

std::string ClientDatanodeTranslator::getHdfsBlockLocations(std::string input) {
  GetHdfsBlockLocationsRequestProto req;
  req.ParseFromString(input);
  logMessage(req, "GetHdfsBlockLocations ");
  const std::string &block_pool_id = req.blockpoolid();
  for (int i = 0; i < req.tokens_size(); i++) {
    const hadoop::common::TokenProto &token = req.tokens(i);
  }
  for (int i = 0; i < req.blockids_size(); i++) {
    const int64_t block_id = req.blockids(i);
  }
  GetHdfsBlockLocationsResponseProto res;
  // TODO(anyone): get HDFS-specific metadata about blocks
  return Serialize(res);
}

std::string ClientDatanodeTranslator::shutdownDatanode(std::string input) {
  ShutdownDatanodeRequestProto req;
  req.ParseFromString(input);
  logMessage(req, "ShutdownDatanode ");
  const bool for_upgrade = req.forupgrade();
  ShutdownDatanodeResponseProto res;
  // TODO(anyone): shut down the datanode. Response contains no fields
  return Serialize(res);
}

std::string ClientDatanodeTranslator::getDatanodeInfo(std::string input) {
  GetDatanodeInfoRequestProto req;
  req.ParseFromString(input);
  logMessage(req, "GetDatanodeInfo ");
  GetDatanodeInfoResponseProto res;
  // TODO(anyone): get datanode info
  return Serialize(res);
}

/**
 * Serialize the message 'res' into out. If the serialization fails, then we
 * must find out to handle it
 * If it succeeds, we simly return the serialized string. 
 */
std::string ClientDatanodeTranslator::Serialize(
      google::protobuf::Message &res) {
  std::string out;
  if (!res.SerializeToString(&out)) {
    // TODO(anyone): handle error
  }
  return out;
}

/**
 * Initialize the rpc server
 */
void ClientDatanodeTranslator::InitServer() {
  LOG(INFO) << "Initializing datanode server...";
  RegisterClientRPCHandlers();
}

/**
 * Register our rpc handlers with the server
 * (See rpcserver.cc - it's where these will get called from)
 */
void ClientDatanodeTranslator::RegisterClientRPCHandlers() {
  // The reason for these binds is because it wants static functions, but we
  // want to give it member functions

  server.register_handler("getReplicaVisibleLength",
                          std::bind(
                            &ClientDatanodeTranslator::getReplicaVisibleLength,
                            this, std::placeholders::_1));
  server.register_handler("refreshNamenodes",
                          std::bind(&ClientDatanodeTranslator::refreshNamenodes,
                                    this, std::placeholders::_1));
  server.register_handler("deleteBlockPool",
                          std::bind(&ClientDatanodeTranslator::deleteBlockPool,
                                    this, std::placeholders::_1));
  server.register_handler("getBlockLocalPathInfo",
                          std::bind(
                            &ClientDatanodeTranslator::getBlockLocalPathInfo,
                            this, std::placeholders::_1));
  server.register_handler("getHdfsBlockLocations",
                          std::bind(
                            &ClientDatanodeTranslator::getHdfsBlockLocations,
                            this, std::placeholders::_1));
  server.register_handler("shutdownDatanode",
                          std::bind(&ClientDatanodeTranslator::shutdownDatanode,
                                    this, std::placeholders::_1));
  server.register_handler("getDatanodeInfo",
                          std::bind(&ClientDatanodeTranslator::getDatanodeInfo,
                                    this, std::placeholders::_1));
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

void ClientDatanodeTranslator::logMessage(google::protobuf::Message &req,
                                          std::string req_name) {
  LOG(INFO) << "Got message " << req_name;
}

}  // namespace client_datanode_translator
