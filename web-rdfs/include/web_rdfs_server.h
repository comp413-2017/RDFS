// Copyright 2017 Rice University, COMP 413 2017

#ifndef WEB_RDFS_INCLUDE_WEB_RDFS_SERVER_H_
#define WEB_RDFS_INCLUDE_WEB_RDFS_SERVER_H_

#include "server_http.hpp"

// Standard HTTP verbs used when registering WebRDFS handlers.
#define HTTP_GET "GET"
#define HTTP_POST "POST"
#define HTTP_PUT "PUT"
#define HTTP_DELETE "DELETE"

using HttpServer = SimpleWeb::Server<SimpleWeb::HTTP>;

class WebRDFSServer {
 public:
  /**
   * Create an WebRDFSServer instance.
   *
   * @param port Port number on which to listen for HTTP requests.
   */
  explicit WebRDFSServer(int16_t port);

  /**
   * Start the WebRDFS server.
   */
  void start();

 private:
  /**
   * Simple-Web-Server HTTP server instance.
   */
  HttpServer server;

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
      std::shared_ptr<HttpServer::Response> response,
      std::shared_ptr<HttpServer::Request> request)> handler);
};

#endif  // WEB_RDFS_INCLUDE_WEB_RDFS_SERVER_H_
