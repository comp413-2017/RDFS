// Copyright 2017 Rice University, COMP 413 2017

#include "web_request_translator.h"

namespace webRequestTranslator {
  /**
   * Converts the RDFS namenode create response into the appropriate webRDFS response.
   */
  std::string getNamenodeCreateResponse(hadoop::hdfs::DatanodeInfoProto
                                        &dataProto,
                                        std::string requestLink) {
    std::string res = std::string("HTTP/1.1 307 TEMPORARY REDIRECT\n");

    std::string delimiter = "/webhfs/v1/";
    std::string restOfRequest = requestLink.substr(requestLink
                                                   .find(delimiter)
                                                   + delimiter.length(),
                                                   requestLink.length());

    hadoop::hdfs::DatanodeIDProto id = dataProto.id();
    res += "Location: http://";
    res += id.hostname();
    res += ":";
    res += std::to_string(id.infoport());

    res += delimiter;
    res += restOfRequest;

    res += "\nContent-Length: 0";
    return res;
  }

  /**
   * Converts the RDFS datanode create response into the appropriate webRDFS response.
   */
  std::string getDatanodeCreateResponse(std::string location,
                                        std::string contentOfFile) {
    std::string res = std::string("HTTP/1.1 200 OK\nLocation: ");

    res += location;
    res += "\n";
    res += "Content-Length: 0";
    //res += std::to_string(contentOfFile.length());
    res += "\n\n";

    return "";
  }

  /**
   * Converts the create response into the appropriate webRDFS response.
   */
   std::string getCreateResponse(std::string path) {
      return path;
   }

  /**
   * Converts the read response into the appropriate webRDFS response.
   */
  std::string getReadResponse(std::string contentOfFile) {
    return contentOfFile;
  }

  /**
   * Converts the RDFS datanode mkdir response into the appropriate webRDFS response.
   */
  std::string getMkdirResponse(zkclient::ZkNnClient::MkdirResponse &resProto) {
    if (resProto == zkclient::ZkNnClient::MkdirResponse::Ok) {
      return "{\"boolean\":true}\n";
    } else {
      return "{\"boolean\":false}\n";
    }
  }

  /**
   * Converts the RDFS datanode rename response into the appropriate webRDFS response.
   */
  std::string getRenameResponse(zkclient::ZkNnClient::RenameResponse
                                &resProto) {
    return "{\"boolean\":true}\n";
  }

  /**
   * Converts the RDFS datanode mv response into the appropriate webRDFS response.
   */
  std::string getMvResponse(hadoop::hdfs::DatanodeInfoProto &dataProto,
                            std::string requestLink) {
    return "{\"boolean\":true}\n";
  }

  /**
   * Converts the RDFS datanode delete response into the appropriate webRDFS response.
   */
  std::string getDeleteResponse(zkclient::ZkNnClient::DeleteResponse
                                &resProto) {
    if (resProto == zkclient::ZkNnClient::DeleteResponse::Ok) {
      return "{\"boolean\":true}\n";
    } else {
      return "{\"boolean\":false}\n";
    }
  }

  /**
   * Converts RDFS response from getFileInfo into the appropriate webRDFS response.
   */
  std::string getFileInfoResponse(zkclient::ZkNnClient::GetFileInfoResponse
                                  &resResp,
                                  hadoop::hdfs::GetFileInfoResponseProto
                                  &resProto) {
    std::string res = std::string("");

    if (resResp ==
        zkclient::ZkNnClient::GetFileInfoResponse::FileDoesNotExist) {
      return "File does not exist\n";
    } else if (resResp ==
               zkclient::ZkNnClient::GetFileInfoResponse::FailedReadZnode) {
      return "Failed to read znode\n";
    }
    res += "{\n\"FileStatus\":\n\n";

    hadoop::hdfs::HdfsFileStatusProto file_status = resProto.fs();

    res += getFileInfoHelper(&file_status);

    res += "\n}\n}\n";

    return res;
  }

  /**
   * Converts RDFS response from getListing into the appropriate webRDFS response.
   */
  std::string getListingResponse(zkclient::ZkNnClient::ListingResponse
                                 &resResp,
                                 hadoop::hdfs::GetListingResponseProto
                                 &resProto) {
    std::string res = std::string("");

    if (resResp ==
        zkclient::ZkNnClient::ListingResponse::FileDoesNotExist) {
      return "File does not exist\n";
    } else if (resResp ==
              zkclient::ZkNnClient::ListingResponse::FailedChildRetrieval) {
      return "Failed to find child\n";
    }

    res += "{\n\"FileStatuses\":\n\"FileStatus\":\n[";

    hadoop::hdfs::DirectoryListingProto dir_listing = resProto.dirlist();
    int i;
    int num_files = dir_listing.partiallisting_size();

    for (i = 0; i < num_files; i++) {
      res += "{\n";
      res += getFileInfoHelper(&dir_listing.partiallisting(i));
      res += "}\n";
    }

    res += "]\n}\n}\n";

    return res;
  }

  /**
   * Gets all the file info from the status and converts it to a string.
   */
  std::string getFileInfoHelper(const hadoop::hdfs::HdfsFileStatusProto
                                *file_status) {
    std::string statusString = std::string("");

    char buf[400];
    int len = 0;

    len += snprintf(buf, sizeof(buf), "\"accessTime\":%ld\n",
                    file_status->access_time());
    len += snprintf(buf + len, sizeof(buf), "\"blockSize\":%ld\n",
                    file_status->blocksize());
    len += snprintf(buf + len, sizeof(buf), "\"group\":%s\n",
                    file_status->group().c_str());
    len += snprintf(buf + len, sizeof(buf), "\"length\":%ld\n",
                    file_status->length());
    len += snprintf(buf + len, sizeof(buf), "\"modificationTime\":%ld\n",
                    file_status->modification_time());
    len += snprintf(buf + len, sizeof(buf), "\"owner\":%s\n",
                    file_status->owner().c_str());
    len += snprintf(buf + len, sizeof(buf), "\"path\":%s\n",
                    file_status->path().c_str());
    len += snprintf(buf + len, sizeof(buf), "\"permission\":%ld\n",
                    file_status->permission().perm());
    len += snprintf(buf + len, sizeof(buf), "\"replication\":%ld\n",
                    file_status->block_replication());
    len += snprintf(buf + len, sizeof(buf), "\"type\":%d\n",
                    file_status->filetype());

    statusString += std::string(buf);

    return statusString;
  }
};  // namespace webRequestTranslator

