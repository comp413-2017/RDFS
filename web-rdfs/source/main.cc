// Copyright 2017 Rice University, COMP 413 2017

#include "rdfs_server.h"

int main(int argc, char *argv[]) {
  LOG(INFO) << "Initializing WebRDFS";

  RDFSServer server(8080);
  server.start();

  return 0;

//  server.config.port = 8080;
//
//  server.resource["^/string$"]["POST"] = [](shared_ptr<HttpServer::Response> response, shared_ptr<HttpServer::Request> request) {
//      auto content = request->content.string();
//
//      *response << "HTTP/1.1 200 OK\r\nContent-Length: " << "3" << "\r\n\r\n"
//                << "asdffsd";
//  };
//
//  server.start();
}
