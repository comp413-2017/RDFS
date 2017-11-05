// Copyright 2017 Rice University, COMP 413 2017

#include "http_handlers.h"

void create_file_handler(std::shared_ptr<HttpServer::Response> response,
                         std::shared_ptr<HttpServer::Request> request) {
  // TODO(security): implement
  *response << "HTTP/1.1 200 OK" << "\r\n\r\n" << "response body";
}
