// Copyright 2017 Rice University, COMP 413 2017

#ifndef WEB_RDFS_INCLUDE_WEB_RDFS_SERVER_H_
#define WEB_RDFS_INCLUDE_WEB_RDFS_SERVER_H_

#include "server_https.h"
#include "zk_nn_client.h"
#include <google/protobuf/extension_set.h>
#include <google/protobuf/generated_enum_reflection.h>
#include <google/protobuf/unknown_field_set.h>
#include <google/protobuf/metadata.h>
#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/arena.h>
#include <google/protobuf/arenastring.h>
#include <google/protobuf/generated_message_util.h>
#include "ClientNamenodeProtocol.pb.h"
#include "hdfs.pb.h"

// Standard HTTP verbs used when registering WebRDFS handlers.
#define HTTP_GET "GET"
#define HTTP_POST "POST"
#define HTTP_PUT "PUT"
#define HTTP_DELETE "DELETE"

// Fully qualified path to certificate and private key used for SSL.
#define SERVER_CERTIFICATE_PATH "/home/vagrant/rdfs/config/keys/server.crt"
#define SERVER_KEY_PATH "/home/vagrant/rdfs/config/keys/server.key"

using HttpsServer = SimpleWeb::Server<SimpleWeb::HTTPS>;

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
using hadoop::hdfs::AppendRequestProto;
using hadoop::hdfs::ConcatRequestProto;
using hadoop::hdfs::SetReplicationRequestProto;
using hadoop::hdfs::GetEZForPathRequestProto;
using hadoop::hdfs::SetOwnerRequestProto;
using hadoop::hdfs::SetPermissionRequestProto;

class WebRDFSServer {
 public:
  /**
   * Create an WebRDFSServer instance.
   *
   * @param port Port number on which to listen for HTTP requests.
   */
  explicit WebRDFSServer(int16_t port, zkclient::ZkNnClient *zk_arg) :
    server(SERVER_CERTIFICATE_PATH, SERVER_KEY_PATH) {
    server.config.port = port;
  };

  /**
   * Start the WebRDFS server.
   */
  void start();

 private:
  /**
   * Simple-Web-Server HTTPS server instance.
   */
  HttpsServer server;

  /**
   * client to communicate with zookeeper
   */
  zkclient::ZkNnClient *zk;

  /**
   * Register an endpoint handler on the server.
   *
   * @param pattern URL pattern to register.
   * @param verb HTTP verb to register.
   * @param handler Handler callback function associated with this endpoint.
   */
  void register_handler(
    std::string pattern,
    const char verb[],
    std::function<void(
      std::shared_ptr<HttpsServer::Response> response,
      std::shared_ptr<HttpsServer::Request> request)> handler);


  std::string getFileInfo(GetFileInfoRequestProto req);
  std::string mkdir(MkdirsRequestProto req);
  std::string destroy(DeleteRequestProto req);
  std::string create(CreateRequestProto req);
  std::string getBlockLocations(GetBlockLocationsRequestProto req);
  std::string getServerDefaults(GetServerDefaultsRequestProto req);
  std::string renewLease(RenewLeaseRequestProto req);
  std::string complete(CompleteRequestProto req);
  std::string setReplication(SetReplicationRequestProto req);
  std::string addBlock(AddBlockRequestProto req);
  std::string getListing(GetListingRequestProto req);
  std::string getEZForPath(GetEZForPathRequestProto req);
  std::string setOwner(SetOwnerRequestProto req);
  std::string getContentSummary(GetContentSummaryRequestProto req);

  std::string rename(RenameRequestProto req);
  std::string rename2(Rename2RequestProto req);
  std::string append(AppendRequestProto req);
  std::string setPermission(SetPermissionRequestProto req);
  std::string recoverLease(RecoverLeaseRequestProto req);
  std::string concat(ConcatRequestProto req);

  std::string abandonBlock(AbandonBlockRequestProto req);
};

#endif  // WEB_RDFS_INCLUDE_WEB_RDFS_SERVER_H_
