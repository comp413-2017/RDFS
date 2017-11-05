// Copyright 2017 Rice University, COMP 413 2017

#include "http_handlers.h"
#include <easylogging++.h>

void create_file_handler(std::shared_ptr<HttpServer::Response> response,
                         std::shared_ptr<HttpServer::Request> request) {
  LOG(DEBUG) << "HTTP request: create_file_handler";

  // TODO(security): implement
  response->write("create_file_handler");
}

void append_file_handler(std::shared_ptr<HttpServer::Response> response,
                         std::shared_ptr<HttpServer::Request> request) {
  LOG(DEBUG) << "HTTP request: append_file_handler";

  // TODO(security): implement
  response->write("append_file_handler");
}

void read_file_handler(std::shared_ptr<HttpServer::Response> response,
                         std::shared_ptr<HttpServer::Request> request) {
  LOG(DEBUG) << "HTTP request: read_file_handler";

  // TODO(security): implement
  response->write("read_file_handler");
}

void get_handler(std::shared_ptr<HttpServer::Response> response,
                 std::shared_ptr<HttpServer::Request> request) {
  // TODO(security): invoke another handler depending on qs opcode.
  create_file_handler(response, request);
}

void post_handler(std::shared_ptr<HttpServer::Response> response,
                  std::shared_ptr<HttpServer::Request> request) {
  // TODO(security): invoke another handler depending on qs opcode.
  create_file_handler(response, request);
}

void put_handler(std::shared_ptr<HttpServer::Response> response,
                 std::shared_ptr<HttpServer::Request> request) {
  // TODO(security): invoke another handler depending on qs opcode.
  create_file_handler(response, request);
}

void delete_handler(std::shared_ptr<HttpServer::Response> response,
                    std::shared_ptr<HttpServer::Request> request) {
  // TODO(security): invoke another handler depending on qs opcode.
  create_file_handler(response, request);
}
