// Copyright 2017 Rice University, COMP 413 2017

#include "http_handlers.h"
#include <cstdlib>
#include <easylogging++.h>
#include <sstream>
#include <iostream>
#include <map>

// Path to the HTML file containing the webRDFS client.
#define WEBRDFS_CLIENT_FILE "/home/vagrant/rdfs/web-rdfs/source/index.html"

zkclient::ZkNnClient *zk;

void setZk(zkclient::ZkNnClient *zk_arg) {
  zk = zk_arg;
}

/**
 * Serve a static file to the client.
 *
 * @param response HTTP response object.
 * @param content_type Content-Type to send to the client.
 * @param static_file_path Full path to the static file to serve.
 */
void serve_static_file(std::shared_ptr<HttpsServer::Response> response,
                       const char *content_type,
                       const char *static_file_path) {
  std::stringstream stream;
  std::string file_contents;

  LOG(DEBUG) << "Serving static file "
             << static_file_path
             << " with Content-Type "
             << content_type;

  // Read the static file contents into memory as text
  std::ifstream static_file(static_file_path);
  std::stringstream buffer;
  buffer << static_file.rdbuf();
  file_contents = buffer.str();

  *response << "HTTP/1.1 200 OK\r\n"
            << "Content-Type: " << content_type << "\r\n"
            << "Content-Length: " << file_contents.length() << "\r\n\r\n"
            << file_contents;
}

std::string get_path(std::shared_ptr<HttpsServer::Request> request) {
  std::string baseUrl = "/webhdfs/v1";
  int idxOfSplit = (request->path).rfind(baseUrl) + baseUrl.size();
  std::string path = (request->path).substr(idxOfSplit);

  LOG(DEBUG) << "Path given " << path;

  return path;
}

std::map<std::string, std::string> parseURL(std::shared_ptr
                                            <HttpsServer::Request> request) {
  LOG(DEBUG) << "Parsing " << request->query_string;

  std::map<std::string, std::string> queryValues;
  queryValues["path"] = get_path(request);
  char *givenValues = strtok((char*)request->query_string.c_str(), "&");

  while (givenValues != NULL)
  {
    std::string currentVal = std::string(givenValues);
    int idxOfSplit = currentVal.rfind("=");
    queryValues[currentVal.substr(0, idxOfSplit)] = currentVal.substr(idxOfSplit + 1);
    givenValues = strtok(NULL, "&");
  }

  return queryValues;
};

void create_file_handler(std::shared_ptr<HttpsServer::Response> response,
                         std::map<std::string, std::string> requestInfo) {
  LOG(DEBUG) << "HTTP request: create_file_handler";

  std::string path = requestInfo["path"];
  std::string input = "hdfs dfs -fs hdfs://localhost:5351 -put " + path;

  hadoop::hdfs::CreateResponseProto res;
  hadoop::hdfs::CreateRequestProto req;

  req.set_src(path);

  zkclient::ZkNnClient::CreateResponse zkResp = zk->create_file(req, res);

  response->write(webRequestTranslator::getCreateResponse(zkResp));
}

void ls_handler(std::shared_ptr<HttpsServer::Response> response,
                std::map<std::string, std::string> requestInfo) {
  LOG(DEBUG) << "HTTP request: ls_handler";

  GetListingRequestProto req;
  GetListingResponseProto res;

  std::string path = requestInfo["path"];
  req.set_src(path);

  zkclient::ZkNnClient::ListingResponse zkResp = zk->get_listing(req, res);

  response->write(webRequestTranslator::getListingResponse(zkResp, res));
}

void append_file_handler(std::shared_ptr<HttpsServer::Response> response,
                         std::map<std::string, std::string> requestInfo) {
  LOG(DEBUG) << "HTTP request: append_file_handler";

  hadoop::hdfs::AppendResponseProto res;
  hadoop::hdfs::AppendRequestProto req;

  std::string path = requestInfo["path"];
  req.set_src(path);

  // TODO(Victoria) change so sends correct data

//  bool isSuccess = zk->append_file(req, res);

  bool isSuccess = true;

  if (isSuccess) {
    response->write(SimpleWeb::StatusCode::success_ok);
  } else {
    response->write(SimpleWeb::StatusCode::server_error_internal_server_error);
  }
}

void set_permission_handler(std::shared_ptr<HttpsServer::Response> response,
                            std::map<std::string, std::string> requestInfo) {
  LOG(DEBUG) << "HTTP request: set_permission_handler";

  hadoop::hdfs::SetPermissionRequestProto req;
  hadoop::hdfs::SetPermissionResponseProto res;

  std::string path = requestInfo["path"];

  req.set_src(path);

  // TODO(Victoria) change so sends correct data

  bool isSuccess = zk->set_permission(req, res);

  if (isSuccess) {
    response->write(SimpleWeb::StatusCode::success_ok);
  } else {
    response->write(SimpleWeb::StatusCode::server_error_internal_server_error);
  }
}

void delete_file_handler(std::shared_ptr<HttpsServer::Response> response,
                         std::map<std::string, std::string> requestInfo) {
  LOG(DEBUG) << "HTTP request: delete_file_handler";

  hadoop::hdfs::DeleteResponseProto res;
  hadoop::hdfs::DeleteRequestProto req;

  std::string path = requestInfo["path"];
  req.set_src(path);
  zkclient::ZkNnClient::DeleteResponse zkResp = zk->destroy(req, res);

  response->write(webRequestTranslator::getDeleteResponse(zkResp));
}

void read_file_handler(std::shared_ptr<HttpsServer::Response> response,
                       std::map<std::string, std::string> requestInfo) {
  LOG(DEBUG) << "HTTP request: read_file_handler";

  std::string path = requestInfo["path"];
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
                   std::map<std::string, std::string> requestInfo) {
  LOG(DEBUG) << "HTTP request: mkdir_handler";

  hadoop::hdfs::MkdirsResponseProto res;
  hadoop::hdfs::MkdirsRequestProto req;

  std::string path = requestInfo["path"];
  req.set_createparent(true);
  req.set_src(path);
  zkclient::ZkNnClient::MkdirResponse zkResp = zk->mkdir(req, res);

  response->write(webRequestTranslator::getMkdirResponse(zkResp));
}

void rename_file_handler(std::shared_ptr<HttpsServer::Response> response,
                         std::map<std::string, std::string> requestInfo) {
  LOG(DEBUG) << "HTTP request: rename_file_handler";

  hadoop::hdfs::RenameResponseProto res;
  hadoop::hdfs::RenameRequestProto req;

  std::string oldPath = requestInfo["path"];
  std::string newPath = requestInfo["destination"];
  req.set_src(oldPath);
  req.set_dst(newPath);
  zkclient::ZkNnClient::RenameResponse zkResp = zk->rename(req, res);

  response->write(webRequestTranslator::getRenameResponse(zkResp));
}

void frontend_handler(std::shared_ptr<HttpsServer::Response> response,
                      std::shared_ptr<HttpsServer::Request> request) {
  LOG(DEBUG) << "Frontend handler invoked";

  serve_static_file(response, "text/html", WEBRDFS_CLIENT_FILE);
}

void get_handler(std::shared_ptr<HttpsServer::Response> response,
                 std::shared_ptr<HttpsServer::Request> request) {
  std::map<std::string, std::string> requestInfo = parseURL(request);
  std::string typeOfRequest = requestInfo["op"];

  if (!typeOfRequest.compare("OPEN")) {
    read_file_handler(response, requestInfo);
  } else if (!typeOfRequest.compare("LISTSTATUS")) {
    ls_handler(response, requestInfo);
  } else {
    response->write(SimpleWeb::StatusCode::client_error_bad_request);
  }
}


void post_handler(std::shared_ptr<HttpsServer::Response> response,
                  std::shared_ptr<HttpsServer::Request> request) {
  std::map<std::string, std::string> requestInfo = parseURL(request);
  std::string typeOfRequest = requestInfo["op"];

  if (!typeOfRequest.compare("APPEND")) {
    append_file_handler(response, requestInfo);
  } else {
    response->write(SimpleWeb::StatusCode::client_error_bad_request);
  }
}

void put_handler(std::shared_ptr<HttpsServer::Response> response,
                 std::shared_ptr<HttpsServer::Request> request) {
  std::map<std::string, std::string> requestInfo = parseURL(request);
  std::string typeOfRequest = requestInfo["op"];

  parseURL(request);
  if (!typeOfRequest.compare("MKDIRS")) {
    mkdir_handler(response, requestInfo);
  } else if (!typeOfRequest.compare("RENAME")) {
    rename_file_handler(response, requestInfo);
  } else if (!typeOfRequest.compare("SETOWNER")) {
    set_permission_handler(response, requestInfo);
  } else if (!typeOfRequest.compare("CREATE")) {
    create_file_handler(response, requestInfo);
  } else {
    response->write(SimpleWeb::StatusCode::client_error_bad_request);
  }
}

void delete_handler(std::shared_ptr<HttpsServer::Response> response,
                    std::shared_ptr<HttpsServer::Request> request) {
  std::map<std::string, std::string> requestInfo = parseURL(request);
  std::string typeOfRequest = requestInfo["op"];

  if (!typeOfRequest.compare("DELETE")) {
    delete_file_handler(response, requestInfo);
  } else {
    response->write(SimpleWeb::StatusCode::client_error_bad_request);
  }
}
