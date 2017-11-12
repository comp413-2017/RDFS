// Copyright 2017 Rice University, COMP 413 2017

#include "http_handlers.h"
#include <cstdlib>
#include <easylogging++.h>

zkclient::ZkNnClient *zk;

void setZk(zkclient::ZkNnClient *zk_arg) {
  zk = zk_arg;
}

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

void delete_file_handler(std::shared_ptr<HttpServer::Response> response,
                         std::string path) {
  LOG(DEBUG) << "HTTP request: delete_file_handler";

  // TODO(security): implement
  hadoop::hdfs::DeleteResponseProto res;
  hadoop::hdfs::DeleteRequestProto req;
  req.set_src(path);
  zkclient::ZkNnClient::DeleteResponse zkResp = zk->destroy(req, res);
  response->write(webRequestTranslator::getDeleteResponse(zkResp));
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
  std::string baseUrl = "/webhdfs/v1/";
  int idxOfSplit = (request->path).rfind(baseUrl) + baseUrl.size();
  std::string rest = (request->path).substr(idxOfSplit);

  int idxOfPath = rest.rfind('/');
  std::string typeOfRequest = rest.substr(0, idxOfPath);
  std::string path = rest.substr(idxOfPath + 1);

  LOG(DEBUG) << typeOfRequest;
  LOG(DEBUG) << path;

  if (typeOfRequest == "delete") {
    delete_file_handler(response, path);
  } else {
    create_file_handler(response, request);
  }
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
