// Copyright 2017 Rice University, COMP 413 2017

#ifndef WEB_RDFS_INCLUDE_HTTP_HANDLERS_H_
#define WEB_RDFS_INCLUDE_HTTP_HANDLERS_H_

#include "server_http.hpp"

using HttpServer = SimpleWeb::Server<SimpleWeb::HTTP>;

void create_file_handler(std::shared_ptr<HttpServer::Response> response,
                         std::shared_ptr<HttpServer::Request> request);

#endif  // WEB_RDFS_INCLUDE_HTTP_HANDLERS_H_
