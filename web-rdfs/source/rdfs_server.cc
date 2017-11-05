// Copyright 2017 Rice University, COMP 413 2017

#include "rdfs_server.h"
#include <easylogging++.h>

#define ELPP_FRESH_LOG_FILE
#define ELPP_THREAD_SAFE

INITIALIZE_EASYLOGGINGPP

void create_file_handler(std::shared_ptr<HttpServer::Response> response,
                         std::shared_ptr<HttpServer::Request> request) {
  // TODO
}

RDFSServer::RDFSServer(unsigned short port) {
  server.config.port = port;
}

void RDFSServer::start() {
  LOG(INFO) << "WebRDFS initializing...";

  // Register all handlers.
  RDFSServer::register_handler("^/webhdfs/v1/.+$", HTTP_PUT, create_file_handler);

  server.start();
}

void RDFSServer::register_handler(
  const std::string pattern,
  const char verb[],
  std::function<void(std::shared_ptr<HttpServer::Response> response,
                     std::shared_ptr<HttpServer::Request> request)> handler) {
  server.resource[pattern][verb] = handler;
}
