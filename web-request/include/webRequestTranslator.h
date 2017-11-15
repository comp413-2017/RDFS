#include <google/protobuf/message.h>
#include "hdfs.pb.h"

#include "zkwrapper.h"
#include "zk_nn_client.h"
#include "zk_dn_client.h"

#include <iostream>
#include <string>
#include <cstdint>
#include <chrono>

#pragma once

namespace webRequestTranslator {
  /**
  * Converts the RDFS namenode create response into the appropriate webRDFS response.
  */
  std::string getNamenodeCreateResponse(hadoop::hdfs::DatanodeInfoProto &dataProto, std::string requestLink);

  /**
  * Converts the RDFS datanode create response into the appropriate webRDFS response.
  */
  std::string getDatanodeCreateResponse(std::string contentOfFile);

  /**
  * Converts the RDFS create response into the appropriate webRDFS response.
  */
  std::string getCreateResponse(std::string path);

  /**
   * Converts the RDFS read response into the appropriate webRDFS response.
   */
  std::string getReadResponse(std::string contentOfFile);

  /**
  * Converts the RDFS datanode mkdir response into the appropriate webRDFS response.
  */
  std::string getMkdirResponse(hadoop::hdfs::DatanodeInfoProto &dataProto, std::string requestLink);

  /**
  * Converts the RDFS datanode mv response into the appropriate webRDFS response.
  */
  std::string getMvResponse(hadoop::hdfs::DatanodeInfoProto &dataProto, std::string requestLink);

  /**
  * Converts the RDFS datanode delete response into the appropriate webRDFS response.
  */
  std::string getDeleteResponse(zkclient::ZkNnClient::DeleteResponse &resProto);

  /**
   * Gets all the file info from the status and converts it to a string.
   */
  std::string getFileInfoHelper(const hadoop::hdfs::HdfsFileStatusProto *file_status);

  /**
   * Converts RDFS response from getFileInfo into the appropriate webRDFS response.
   */
  std::string getFileInfoResponse(zkclient::ZkNnClient::GetFileInfoResponse &resResp,
                                hadoop::hdfs::GetFileInfoResponseProto &resProto);

  /**
   * Converts RDFS response from getListing into the appropriate webRDFS response.
   */
  std::string getListingResponse(zkclient::ZkNnClient::ListingResponse &resResp,
                                hadoop::hdfs::GetListingResponseProto &resProto);

  /**
   * Gets all the file info from the status and converts it to a string.
   */
  std::string getFileInfoHelper(const hadoop::hdfs::HdfsFileStatusProto *file_status);
}; // namespace
