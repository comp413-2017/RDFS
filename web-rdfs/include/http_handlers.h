// Copyright 2017 Rice University, COMP 413 2017

#ifndef WEB_RDFS_INCLUDE_HTTP_HANDLERS_H_
#define WEB_RDFS_INCLUDE_HTTP_HANDLERS_H_

#include "server_http.h"
#include "web_rdfs_server.h"
#include "web_request_translator.h"

#include <string>
#include <fstream>
#include <streambuf>

using HttpsServer = SimpleWeb::Server<SimpleWeb::HTTPS>;

void setZk(zkclient::ZkNnClient *zk_arg);

/**
 * Handler invoked when a client issues a GET request for an RDFS path.
 *
 * @param response HTTP response object.
 * @param request HTTP request object.
 */
void get_handler(std::shared_ptr<HttpsServer::Response> response,
                 std::shared_ptr<HttpsServer::Request> request);

/**
 * Handler invoked when a client issues a POST request for an RDFS path.
 *
 * @param response HTTP response object.
 * @param request HTTP request object.
 */
void post_handler(std::shared_ptr<HttpsServer::Response> response,
                  std::shared_ptr<HttpsServer::Request> request);

/**
 * Handler invoked when a client issues a PUT request for an RDFS path.
 *
 * @param response HTTP response object.
 * @param request HTTP request object.
 */
void put_handler(std::shared_ptr<HttpsServer::Response> response,
                 std::shared_ptr<HttpsServer::Request> request);

/**
 * Handler invoked when a client issues a DELETE request for an RDFS path.
 *
 * @param response HTTP response object.
 * @param request HTTP request object.
 */
void delete_handler(std::shared_ptr<HttpsServer::Response> response,
                    std::shared_ptr<HttpsServer::Request> request);

#endif  // WEB_RDFS_INCLUDE_HTTP_HANDLERS_H_
