// Copyright 2017 Rice University, COMP 413 2017

#include "web_rdfs_server.h"
#include <easylogging++.h>
#include "http_handlers.h"

WebRDFSServer::WebRDFSServer(int16_t port) {
  LOG(INFO) << "WebRDFS listening on port " << port;
  server.config.port = port;
}

void WebRDFSServer::start() {
  LOG(INFO) << "WebRDFS starting...";

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

  server.start();
}

void WebRDFSServer::register_handler(
  const std::string pattern,
  const char verb[],
  std::function<void(std::shared_ptr<HttpServer::Response> response,
                     std::shared_ptr<HttpServer::Request> request)> handler) {
  LOG(DEBUG) << "WebRDFS registering handler: "
             << verb
             << ", "
             << pattern;
  server.resource[pattern][verb] = handler;
}
