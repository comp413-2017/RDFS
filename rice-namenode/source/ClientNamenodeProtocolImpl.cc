// Copyright 2017 Rice University, COMP 413 2017

#include "ClientNamenodeProtocolImpl.h"

#include <hdfs.pb.h>

#include <easylogging++.h>
#include <rpcserver.h>
#include <zkwrapper.h>
#include <google/protobuf/extension_set.h>
#include <google/protobuf/generated_enum_reflection.h>
#include <google/protobuf/unknown_field_set.h>
#include <google/protobuf/metadata.h>
#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/arena.h>
#include <google/protobuf/arenastring.h>
#include <google/protobuf/generated_message_util.h>
#include <unistd.h>

#include <iostream>
#include <string>

#include <thread>

#include <RpcHeader.pb.h>
#include <ConfigReader.h>
#include <ClientNamenodeProtocol.pb.h>

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
using hadoop::hdfs::GetFileInfoRequestProto;
using hadoop::hdfs::GetFileInfoResponseProto;
using hadoop::hdfs::MkdirsRequestProto;
using hadoop::hdfs::MkdirsResponseProto;
using hadoop::hdfs::DeleteRequestProto;
using hadoop::hdfs::DeleteResponseProto;
using hadoop::hdfs::CreateRequestProto;
using hadoop::hdfs::CreateResponseProto;
using hadoop::hdfs::GetBlockLocationsRequestProto;
using hadoop::hdfs::GetBlockLocationsResponseProto;
using hadoop::hdfs::GetServerDefaultsRequestProto;
using hadoop::hdfs::GetServerDefaultsResponseProto;
using hadoop::hdfs::FsServerDefaultsProto;
using hadoop::hdfs::RenewLeaseRequestProto;
using hadoop::hdfs::RenewLeaseResponseProto;
using hadoop::hdfs::CompleteRequestProto;
using hadoop::hdfs::CompleteResponseProto;
using hadoop::hdfs::AbandonBlockRequestProto;
using hadoop::hdfs::AbandonBlockResponseProto;
using hadoop::hdfs::AddBlockRequestProto;
using hadoop::hdfs::AddBlockResponseProto;
using hadoop::hdfs::RenameRequestProto;
using hadoop::hdfs::RenameResponseProto;
using hadoop::hdfs::SetPermissionResponseProto;
using hadoop::hdfs::GetListingRequestProto;
using hadoop::hdfs::GetListingResponseProto;
using hadoop::hdfs::SetReplicationResponseProto;
using hadoop::hdfs::GetEZForPathResponseProto;
using hadoop::hdfs::SetOwnerResponseProto;
using hadoop::hdfs::GetContentSummaryRequestProto;
using hadoop::hdfs::GetContentSummaryResponseProto;
using hadoop::hdfs::RecoverLeaseRequestProto;
using hadoop::hdfs::RecoverLeaseResponseProto;
using hadoop::hdfs::Rename2RequestProto;
using hadoop::hdfs::Rename2ResponseProto;

ClientNamenodeTranslator::ClientNamenodeTranslator(
    int port_arg,
    zkclient::ZkNnClient *zk_arg
) : port(port_arg), server(port), zk(zk_arg) {
  InitServer();
  LOG(INFO) << "Created client namenode translator.";
}


// ----------------------- RPC HANDLERS ----------------------------





std::string ClientNamenodeTranslator::append(std::string input) {
  return "";
}

std::string ClientNamenodeTranslator::fsync(std::string input) {
  return "";
}

std::string ClientNamenodeTranslator::finalizeUpgrade(std::string input) {
  return "";
}

std::string ClientNamenodeTranslator::getFileInfo(std::string input) {
  GetFileInfoRequestProto req;
  req.ParseFromString(input);
  logMessage(&req, "GetFileInfo ");
  GetFileInfoResponseProto res;
  switch (zk->get_info(req, res)) {
    case zkclient::ZkNnClient::GetFileInfoResponse::Ok:
      logMessage(&res, "GetFileInfo response ");
      break;
    case zkclient::ZkNnClient::GetFileInfoResponse::FileDoesNotExist:
      LOG(INFO) << "get_info returned FileDoesNotExist";
      break;
    case zkclient::ZkNnClient::GetFileInfoResponse::FailedReadZnode:
      LOG(INFO) << "get_info returned FailedReadZnode";
      break;
  }
  return Serialize(res);
}

std::string ClientNamenodeTranslator::mkdir(std::string input) {
  TIMED_FUNC_IF(destroyHandlerTimer, VLOG_IS_ON(9));
  MkdirsRequestProto req;
  req.ParseFromString(input);
  logMessage(&req, "Mkdir ");
  MkdirsResponseProto res;
  if (zk->mkdir(req, res) == zkclient::ZkNnClient::MkdirResponse::Ok) {
    return Serialize(res);
  } else {
    throw GetErrorRPCHeader("Could not mkdir", "");
  }
}

std::string ClientNamenodeTranslator::destroy(std::string input) {
  TIMED_FUNC_IF(destroyTimer, VLOG_IS_ON(9));

  DeleteRequestProto req;
  req.ParseFromString(input);
  logMessage(&req, "Delete ");
  const std::string &src = req.src();
  const bool recursive = req.recursive();
  DeleteResponseProto res;
  if (zk->destroy(req, res) == zkclient::ZkNnClient::DeleteResponse::Ok) {
    return Serialize(res);
  } else {
    throw GetErrorRPCHeader("Could not delete", "");
  }
}

std::string ClientNamenodeTranslator::create(std::string input) {
  CreateRequestProto req;
  req.ParseFromString(input);
  logMessage(&req, "Create ");
  CreateResponseProto res;
  if (zk->create_file(req, res) == zkclient::ZkNnClient::CreateResponse::Ok) {
  } else {
    throw GetErrorRPCHeader("Could not create file", "");
  }
  return Serialize(res);
}

std::string ClientNamenodeTranslator::getBlockLocations(std::string input) {
  GetBlockLocationsRequestProto req;
  req.ParseFromString(input);
  logMessage(&req, "GetBlockLocations ");
  GetBlockLocationsResponseProto res;
  zk->get_block_locations(req, res);
  return Serialize(res);
}

std::string ClientNamenodeTranslator::getServerDefaults(std::string input) {
  GetServerDefaultsRequestProto req;
  req.ParseFromString(input);
  logMessage(&req, "GetServerDefaults");
  GetServerDefaultsResponseProto res;
  FsServerDefaultsProto *def = res.mutable_serverdefaults();
  // read all this config info
  def->set_blocksize(getDefaultInt("dfs.blocksize"));
  def->set_bytesperchecksum(getDefaultInt("dfs.bytes-per-checksum"));
  def->set_writepacketsize(getDefaultInt("dfs.client-write-packet-size"));
  def->set_replication(getDefaultInt("dfs.replication"));
  def->set_filebuffersize(getDefaultInt("dfs.stream-buffer-size"));
  def->set_encryptdatatransfer(getDefaultInt("dfs.encrypt.data.transfer"));
  // TODO(2016): ChecksumTypeProto (optional)
  return Serialize(res);
}

std::string ClientNamenodeTranslator::renewLease(std::string input) {
  RenewLeaseRequestProto req;
  req.ParseFromString(input);
  logMessage(&req, "RenewLease ");
  RenewLeaseResponseProto res;
  return Serialize(res);
}

std::string ClientNamenodeTranslator::complete(std::string input) {
  CompleteRequestProto req;
  req.ParseFromString(input);
  logMessage(&req, "Complete ");
  CompleteResponseProto res;
  // TODO(2016) some optional fields need to be read
  zk->complete(req, res);
  return Serialize(res);
}

/**
 * The client can give up on a block by calling abandonBlock().
 * The client can then either obtain a new block, or complete or
 * abandon the file. Any partial writes to the block will be discarded.
 */
std::string ClientNamenodeTranslator::abandonBlock(std::string input) {
  AbandonBlockRequestProto req;
  AbandonBlockResponseProto res;
  req.ParseFromString(input);
  logMessage(&req, "AbandonBlock ");
  if (zk->abandon_block(req, res)) {
    return Serialize(res);
  } else {
    throw GetErrorRPCHeader("Could not abandon block", "");
  }
}

std::string ClientNamenodeTranslator::addBlock(std::string input) {
  AddBlockRequestProto req;
  AddBlockResponseProto res;
  req.ParseFromString(input);
  logMessage(&req, "AddBlock ");
  if (zk->add_block(req, res)) {
    return Serialize(res);
  } else {
    throw GetErrorRPCHeader("Could not add block", "");
  }
}

std::string ClientNamenodeTranslator::rename(std::string input) {
  RenameRequestProto req;
  RenameResponseProto res;
  req.ParseFromString(input);
  logMessage(&req, "Rename ");
  zk->rename(req, res);
  return Serialize(res);
}

std::string ClientNamenodeTranslator::setPermission(std::string input) {
  SetPermissionResponseProto res;
  return Serialize(res);
}

std::string ClientNamenodeTranslator::getListing(std::string input) {
  GetListingRequestProto req;
  GetListingResponseProto res;
  req.ParseFromString(input);
  logMessage(&req, "GetListing ");
  if (zk->get_listing(req, res) == zkclient::ZkNnClient::ListingResponse::Ok) {
    return Serialize(res);
  } else {
    throw GetErrorRPCHeader("Could not get listing", "");
  }
}

std::string ClientNamenodeTranslator::setReplication(std::string input) {
  SetReplicationResponseProto res;
  res.set_result(1);
  return Serialize(res);
}

std::string ClientNamenodeTranslator::getEZForPath(std::string input) {
  GetEZForPathResponseProto res;
  return Serialize(res);
}

std::string ClientNamenodeTranslator::setOwner(std::string input) {
  SetOwnerResponseProto res;
  return Serialize(res);
}

std::string ClientNamenodeTranslator::getContentSummary(std::string input) {
  GetContentSummaryRequestProto req;
  req.ParseFromString(input);
  GetContentSummaryResponseProto res;
  zk->get_content(req, res);
  return Serialize(res);
}
/**
 * While we expect clients to renew their lease, we should never allow
 * a client to "recover" a lease, since we only allow a write-once system
 */
std::string ClientNamenodeTranslator::recoverLease(std::string input) {
  RecoverLeaseRequestProto req;
  req.ParseFromString(input);
  logMessage(&req, "RecoverLease ");
  RecoverLeaseResponseProto res;
  // just tell the client they could not recover the lease, so they won't try
  // and write
  res.set_result(false);
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
 * That being said, the following is a short list of some common commands we
 * don't support, and our reasons for not supporting them:
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
 * TODO(2016): - might support this later!
 *
 */

// ------------------------------ TODO(2016): ------------------------------



// TODO(2016): what is this? It originally was inside the "commands not
// supported" block, but it seems to be doing something?
std::string ClientNamenodeTranslator::rename2(std::string input) {
  Rename2RequestProto req;
  req.ParseFromString(input);
  logMessage(&req, "Rename2 ");
  Rename2ResponseProto res;
  return Serialize(res);
}



// ----------------------- HANDLER HELPERS --------------------------------

/**
 * Serialize the message 'res' into out. If the serialization fails, then we
 * must find out to handle it If it succeeds, we simly return the serialized
 * string.
 */
std::string ClientNamenodeTranslator::Serialize(
    google::protobuf::Message &res
) {
  std::string out;
  logMessage(&res, "Responding with ");
  if (!res.SerializeToString(&out)) {
    // TODO(2016): handle error
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
hadoop::common::RpcResponseHeaderProto ClientNamenodeTranslator::
GetErrorRPCHeader(std::string error_msg,
                  std::string exception_classname
) {
  hadoop::common::RpcResponseHeaderProto response_header;
  response_header.set_status(
      hadoop::common::
      RpcResponseHeaderProto_RpcStatusProto_ERROR);
  response_header.set_errormsg(error_msg);
  response_header.set_exceptionclassname(exception_classname);
  // TODO(2016): since this method is now only being used for failed handlers,
  // this line seems to be incorrect. As far as I can tell, only create uses
  // this method now.
  response_header.set_errordetail(
      hadoop::common::
      RpcResponseHeaderProto_RpcErrorCodeProto_ERROR_APPLICATION);
  return response_header;
}

// --------------------- CONFIG AND INITIALIZATION ----------------------

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

// ------------------------- RPC SERVER INTERACTIONS ----------------------

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
  using std::placeholders::_1;

  // The reason for these binds is because it wants static functions, but we
  // want to give it member functions
  // http://stackoverflow.com/questions/14189440/c-class-member-callback
  // -simple-examples
  server.register_handler(
      "getFileInfo",
      std::bind(&ClientNamenodeTranslator::getFileInfo, this, _1));
  server.register_handler(
      "mkdirs",
      std::bind(&ClientNamenodeTranslator::mkdir, this, _1));
  server.register_handler(
      "delete",
      std::bind(&ClientNamenodeTranslator::destroy, this, _1));
  server.register_handler(
      "create",
      std::bind(&ClientNamenodeTranslator::create, this, _1));
  server.register_handler(
      "abandonBlock",
      std::bind(&ClientNamenodeTranslator::abandonBlock, this, _1));
  server.register_handler(
      "renewLease",
      std::bind(&ClientNamenodeTranslator::renewLease, this, _1));
  server.register_handler(
      "getServerDefaults",
      std::bind(&ClientNamenodeTranslator::getServerDefaults, this, _1));
  server.register_handler(
      "complete",
      std::bind(&ClientNamenodeTranslator::complete, this, _1));
  server.register_handler(
      "getBlockLocations",
      std::bind(&ClientNamenodeTranslator::getBlockLocations, this, _1));
  server.register_handler(
      "addBlock",
      std::bind(&ClientNamenodeTranslator::addBlock, this, _1));

  // TODO(2016): what is this function for? Do we still need it??
  server.register_handler(
      "rename2",
      std::bind(&ClientNamenodeTranslator::rename2, this, _1));
  server.register_handler(
      "rename",
      std::bind(&ClientNamenodeTranslator::rename, this, _1));
  server.register_handler(
      "recoverLease",
      std::bind(&ClientNamenodeTranslator::recoverLease, this, _1));
  server.register_handler(
      "setPermission",
      std::bind(&ClientNamenodeTranslator::setPermission, this, _1));

  server.register_handler(
      "setReplication",
      std::bind(&ClientNamenodeTranslator::setReplication, this, _1));
  server.register_handler(
      "getListing",
      std::bind(&ClientNamenodeTranslator::getListing, this, _1));
  server.register_handler(
      "getEZForPath",
      std::bind(&ClientNamenodeTranslator::getEZForPath, this, _1));
  server.register_handler(
      "setOwner",
      std::bind(&ClientNamenodeTranslator::setOwner, this, _1));
  server.register_handler(
      "getContentSummary",
      std::bind(&ClientNamenodeTranslator::getContentSummary, this, _1));

  // Additional methods to support for FM - marc, pradhith, anthony
  server.register_handler(
      "append",
      std::bind(&ClientNamenodeTranslator::append, this, _1));

  server.register_handler(
      "fsync",
      std::bind(&ClientNamenodeTranslator::fsync, this, _1));

  server.register_handler(
      "finalizeUpgrade",
      std::bind(&ClientNamenodeTranslator::finalizeUpgrade, this, _1));
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

// ------------------------------- HELPERS -----------------------------

void ClientNamenodeTranslator::logMessage(
    google::protobuf::Message *req,
    std::string req_name
) {
  LOG(INFO) << "Got message " << req_name;
}

ClientNamenodeTranslator::~ClientNamenodeTranslator() {
  // TODO(2016): handle being shut down
}
}  // namespace client_namenode_translator
