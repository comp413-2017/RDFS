// Copyright 2017 Rice University, COMP 413 2017

#ifndef WEB_RDFS_INCLUDE_RDFS_SERVER_H_
#define WEB_RDFS_INCLUDE_RDFS_SERVER_H_

#include "server_http.hpp"

// Standard HTTP verbs used when registering WebRDFS handlers.
#define HTTP_GET "GET"
#define HTTP_POST "POST"
#define HTTP_PUT "PUT"
#define HTTP_DELETE "DELETE"

using HttpServer = SimpleWeb::Server<SimpleWeb::HTTP>;

class RDFSServer {
public:
  /**
   * TODO
   *
   * @param port
   */
  explicit RDFSServer(unsigned short port);

  /**
   * TODO
   */
  void start();

private:
  /**
   * TODO
   */
  HttpServer server;

  void register_handler(const std::string pattern, const char verb[],
                        std::function<void(
                          std::shared_ptr<HttpServer::Response> response,
                          std::shared_ptr<HttpServer::Request> request)> handler);
};

#endif  // WEB_RDFS_INCLUDE_RDFS_SERVER_H_
