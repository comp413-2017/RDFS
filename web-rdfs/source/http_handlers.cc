// Copyright 2017 Rice University, COMP 413 2017

#include "http_handlers.h"
#include <cstdlib>
#include <easylogging++.h>
#include <sstream>
#include <iostream>

zkclient::ZkNnClient *zk;

void setZk(zkclient::ZkNnClient *zk_arg) {
  zk = zk_arg;
}

std::string get_request_type(std::shared_ptr<HttpsServer::Request> request) {
  // Remove op= from query string
  std::string typeOfRequest = request->query_string.substr(3, 6);

  LOG(DEBUG) << "Type of Request " << typeOfRequest;

  return typeOfRequest;
}

std::string get_path(std::shared_ptr<HttpsServer::Request> request) {
  std::string baseUrl = "/webhdfs/v1";
  int idxOfSplit = (request->path).rfind(baseUrl) + baseUrl.size();
  std::string path = (request->path).substr(idxOfSplit);

  LOG(DEBUG) << "Path given " << path;

  return path;
}

std::string get_destination(std::shared_ptr<HttpsServer::Request> request) {
  std::string destDelim = "&destination=";
  int idxOfDest = request->query_string.rfind(destDelim) + destDelim.size();
  std::string dest = request->query_string.substr(idxOfDest);

  LOG(DEBUG) << "Destination given " << dest;

  return dest;
}

void create_file_handler(std::shared_ptr<HttpsServer::Response> response,
                         std::shared_ptr<HttpsServer::Request> request) {
  LOG(DEBUG) << "HTTP request: create_file_handler";

  // TODO(security): implement
  response->write("create_file_handler");
}

void append_file_handler(std::shared_ptr<HttpsServer::Response> response,
                         std::shared_ptr<HttpsServer::Request> request) {
  LOG(DEBUG) << "HTTP request: append_file_handler";

  // TODO(security): implement
  response->write("append_file_handler");
}

void delete_file_handler(std::shared_ptr<HttpsServer::Response> response,
                         std::string path) {
  LOG(DEBUG) << "HTTP request: delete_file_handler";

  hadoop::hdfs::DeleteResponseProto res;
  hadoop::hdfs::DeleteRequestProto req;

  req.set_src(path);
  zkclient::ZkNnClient::DeleteResponse zkResp = zk->destroy(req, res);

  response->write(webRequestTranslator::getDeleteResponse(zkResp));
}

void read_file_handler(std::shared_ptr<HttpsServer::Response> response,
                         std::string path) {
  LOG(DEBUG) << "HTTP request: read_file_handler";

  std::string storedFile = "tempStore" + path;
  std::string input = "hdfs dfs -fs hdfs://localhost:5351 -cat " + path +
                      " > " + storedFile;

  system(input.c_str());
  std::ifstream file(storedFile);
  std::string content((std::istreambuf_iterator<char>(file)),
                       std::istreambuf_iterator<char>());

  LOG(DEBUG) << content;

  response->write(webRequestTranslator::getReadResponse(content));

  system(("rm " + storedFile).c_str());  // Clean up temp file
}

void mkdir_handler(std::shared_ptr<HttpsServer::Response> response,
                         std::string path) {
  LOG(DEBUG) << "HTTP request: mkdir_handler";

  hadoop::hdfs::MkdirsResponseProto res;
  hadoop::hdfs::MkdirsRequestProto req;

  req.set_createparent(true);
  req.set_src(path);
  zkclient::ZkNnClient::MkdirResponse zkResp = zk->mkdir(req, res);

  response->write(webRequestTranslator::getMkdirResponse(zkResp));
}

void rename_file_handler(std::shared_ptr<HttpsServer::Response> response,
                         std::string oldPath,
                         std::string newPath) {
  LOG(DEBUG) << "HTTP request: rename_file_handler";

  hadoop::hdfs::RenameResponseProto res;
  hadoop::hdfs::RenameRequestProto req;

  req.set_src(oldPath);
  req.set_dst(newPath);
  zkclient::ZkNnClient::RenameResponse zkResp = zk->rename(req, res);

  response->write(webRequestTranslator::getRenameResponse(zkResp));
}

void get_handler(std::shared_ptr<HttpsServer::Response> response,
                 std::shared_ptr<HttpsServer::Request> request) {
  // TODO(security): invoke another handler depending on qs opcode.
  std::string baseUrl = "/webhdfs/v1";

  hadoop::hdfs::MkdirsResponseProto res;
  hadoop::hdfs::MkdirsRequestProto req;

  req.set_createparent(true);
  req.set_src(path);
  zkclient::ZkNnClient::MkdirResponse zkResp = zk->mkdir(req, res);

  response->write(webRequestTranslator::getMkdirResponse(zkResp));
}

void rename_file_handler(std::shared_ptr<HttpsServer::Response> response,
                         std::string oldPath,
                         std::string newPath) {
  LOG(DEBUG) << "HTTP request: rename_file_handler";

  hadoop::hdfs::RenameResponseProto res;
  hadoop::hdfs::RenameRequestProto req;

  req.set_src(oldPath);
  req.set_dst(newPath);
  zkclient::ZkNnClient::RenameResponse zkResp = zk->rename(req, res);

  response->write(webRequestTranslator::getRenameResponse(zkResp));
}

void get_handler(std::shared_ptr<HttpsServer::Response> response,
                 std::shared_ptr<HttpsServer::Request> request) {
  std::string typeOfRequest = get_request_type(request);
  std::string path = get_path(request);

  if (!typeOfRequest.compare("OPEN")) {
    read_file_handler(response, path);
  } else if (!typeOfRequest.compare("MKDIR")) {
    mkdir_handler(response, path);
  } else if (!typeOfRequest.find("RENAME")) {
    std::string pathForRename = typeOfRequest.substr(6);
    rename_file_handler(response, path, pathForRename);
  } else {
    response->write(SimpleWeb::StatusCode::client_error_bad_request);
  }
}

void post_handler(std::shared_ptr<HttpsServer::Response> response,
                  std::shared_ptr<HttpsServer::Request> request) {
  // Do not support post requests right now
  response->write(SimpleWeb::StatusCode::client_error_bad_request);
}

void put_handler(std::shared_ptr<HttpsServer::Response> response,
                 std::shared_ptr<HttpsServer::Request> request) {
  std::string typeOfRequest = get_request_type(request);
  std::string path = get_path(request);

  if (!typeOfRequest.compare("MKDIRS")) {
    mkdir_handler(response, path);
  } else if (!typeOfRequest.compare("RENAME")) {
    std::string pathForRename = get_destination(request);
    rename_file_handler(response, path, pathForRename);
  } else {
    response->write(SimpleWeb::StatusCode::client_error_bad_request);
  }
}

void delete_handler(std::shared_ptr<HttpsServer::Response> response,
                    std::shared_ptr<HttpsServer::Request> request) {
  // Remove op= from query string
  std::string typeOfRequest = get_request_type(request);
  std::string path = get_path(request);

  if (!typeOfRequest.compare("DELETE")) {
    delete_file_handler(response, path);
  } else {
    response->write(SimpleWeb::StatusCode::client_error_bad_request);
  }
}
