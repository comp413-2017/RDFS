#include <google/protobuf/message.h>
#include "hdfs.pb.h"

#include "zk_nn_client.h"
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
   * Converts the RDFS namenode info response into the appropriate webRDFS response.
   */
  std::string getNamenodeReadResponse(hadoop::hdfs::DatanodeInfoProto &dataProto, std::string requestLink);

  /**
   * Converts the RDFS datanode read response into the appropriate webRDFS response.
   */
  std::string getDatanodeReadResponse(std::string contentOfFile);

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