// Copyright 2017 Rice University, COMP 413 2017

#include <google/protobuf/message.h>
#include "hdfs.pb.h"

#include "zkwrapper.h"
#include "zk_nn_client.h"
#include "zk_dn_client.h"
#include "server_http.h"

#include <iostream>
#include <string>
#include <cstdint>
#include <chrono>

#pragma once

namespace webRequestTranslator {
  /**
  * Converts the RDFS create response into the appropriate webRDFS response.
  */
  SimpleWeb::StatusCode getCreateResponse(int is_failure);

  /**
    * Converts the RDFS append response into the appropriate webRDFS response.
    */
  SimpleWeb::StatusCode getAppendResponse(int is_failure);

  /**
   * Converts the RDFS read response into the appropriate webRDFS response.
   */
  std::string getReadResponse(std::string contentOfFile, int is_failure);

  /**
  * Converts the RDFS datanode mkdir response into the appropriate webRDFS response.
  */
  std::string getMkdirResponse(int is_failure);

  /**
   * Converts the RDFS datanode rename response into the appropriate webRDFS response.
   */
  std::string getRenameResponse(int is_failure);

  /**
  * Converts the RDFS datanode mv response into the appropriate webRDFS response.
  */
  std::string getMvResponse(hadoop::hdfs::DatanodeInfoProto &dataProto,
                            std::string requestLink);

  /**
  * Converts the RDFS datanode delete response into the appropriate webRDFS response.
  */
  std::string getDeleteResponse(int is_failure);

  /**
   * Gets all the file info from the status and converts it to a string.
   */
  std::string getFileInfoHelper(const hadoop::hdfs::HdfsFileStatusProto
                                *file_status);

  /**
   * Converts RDFS response from getFileInfo into the appropriate webRDFS response.
   */
  std::string getFileInfoResponse(zkclient::ZkNnClient::GetFileInfoResponse
                                  &resResp,
                                  hadoop::hdfs::GetFileInfoResponseProto
                                  &resProto);

  /**
   * Converts RDFS response from getListing into the appropriate webRDFS response.
   */
  std::string getListingResponse(zkclient::ZkNnClient::ListingResponse
                                 &resResp,
                                 hadoop::hdfs::GetListingResponseProto
                                 &resProto);

  /**
   * Gets all the file info from the status and converts it to a string.
   */
  std::string getFileInfoHelper(const hadoop::hdfs::HdfsFileStatusProto
                                *file_status);
};  // namespace webRequestTranslator
