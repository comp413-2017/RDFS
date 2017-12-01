// Copyright 2017 Rice University, COMP 413 2017

#include "web_rdfs_server.h"
#include <easylogging++.h>
#include "http_handlers.h"

void WebRDFSServer::start() {
  LOG(INFO) << "WebRDFS listening on port " << server.config.port;

  // Register all handlers.
  WebRDFSServer::register_handler("^/webhdfs/v1/.+$",
                                  HTTP_GET,
                                  get_handler);
  WebRDFSServer::register_handler("^/webhdfs/v1/.+$",
                                  HTTP_POST,
                                  post_handler);
  WebRDFSServer::register_handler("^/webhdfs/v1/.+$",
                                  HTTP_PUT,
                                  put_handler);
  WebRDFSServer::register_handler("^/webhdfs/v1/.+$",
                                  HTTP_DELETE,
                                  delete_handler);
  WebRDFSServer::register_handler("^/$",
                                  HTTP_GET,
                                  frontend_handler);

  server.start();
}

void WebRDFSServer::register_handler(
  const std::string pattern,
  const char verb[],
  std::function<void(std::shared_ptr<HttpsServer::Response> response,
                     std::shared_ptr<HttpsServer::Request> request)> handler) {
  LOG(DEBUG) << "WebRDFS registering handler: "
             << verb
             << ", "
             << pattern;
  server.resource[pattern][verb] = handler;
}

std::string WebRDFSServer::getFileInfo(GetFileInfoRequestProto req) {
  GetFileInfoResponseProto res;
  return "";
}

std::string WebRDFSServer::mkdir(MkdirsRequestProto req) {
  MkdirsResponseProto res;
  return "";
}

std::string WebRDFSServer::create(CreateRequestProto req) {
  CreateResponseProto res;
  return "";
}

std::string WebRDFSServer::getBlockLocations(
  GetBlockLocationsRequestProto req) {
  GetBlockLocationsResponseProto res;
  return "";
}

std::string WebRDFSServer::getServerDefaults(
  GetServerDefaultsRequestProto req) {
  GetServerDefaultsResponseProto res;
  // FsServerDefaultsProto *def = res.mutable_serverdefaults();
  // // read all this config info
  // def->set_blocksize(getDefaultInt("dfs.blocksize"));
  // def->set_bytesperchecksum(getDefaultInt("dfs.bytes-per-checksum"));
  // def->set_writepacketsize(getDefaultInt("dfs.client-write-packet-size"));
  // def->set_replication(getDefaultInt("dfs.replication"));
  // def->set_filebuffersize(getDefaultInt("dfs.stream-buffer-size"));
  // def->set_encryptdatatransfer(getDefaultInt("dfs.encrypt.data.transfer"));
  return "";
}

std::string WebRDFSServer::renewLease(RenewLeaseRequestProto req) {
  RenewLeaseResponseProto res;
  return "";
}

std::string WebRDFSServer::complete(CompleteRequestProto req) {
  CompleteResponseProto res;
  return "";
}

std::string WebRDFSServer::setReplication(SetReplicationRequestProto req) {
  SetReplicationResponseProto res;
  res.set_result(1);
  return "";
}

std::string WebRDFSServer::addBlock(AddBlockRequestProto req) {
  AddBlockResponseProto res;
  return "";
}

std::string WebRDFSServer::getListing(GetListingRequestProto req) {
  GetListingResponseProto res;
  return "";
}

std::string WebRDFSServer::getEZForPath(GetEZForPathRequestProto req) {
  GetEZForPathResponseProto res;
  return "";
}

std::string WebRDFSServer::setOwner(SetOwnerRequestProto req) {
  SetOwnerResponseProto res;
  return "";
}

std::string WebRDFSServer::getContentSummary(
  GetContentSummaryRequestProto req) {
  GetContentSummaryResponseProto res;
  return "";
}

std::string WebRDFSServer::rename(RenameRequestProto req) {
  RenameResponseProto res;
  return "";
}

std::string WebRDFSServer::rename2(Rename2RequestProto req) {
  Rename2ResponseProto res;
  return "";
}

std::string WebRDFSServer::append(AppendRequestProto req) {
  return "";
}

std::string WebRDFSServer::setPermission(SetPermissionRequestProto req) {
  SetPermissionResponseProto res;
  return "";
}

std::string WebRDFSServer::recoverLease(RecoverLeaseRequestProto req) {
  RecoverLeaseResponseProto res;
  // just tell the client they could not recover the lease, so they won't try
  // and write
  res.set_result(false);
  return "";
}

std::string WebRDFSServer::concat(ConcatRequestProto req) {
  return "";
}

std::string WebRDFSServer::abandonBlock(AbandonBlockRequestProto req) {
  AbandonBlockResponseProto res;
  return "";
}
