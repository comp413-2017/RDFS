// Copyright 2017 Rice University, COMP 413 2017

#include <limits>
#include "NameNodeTest.h"


TEST_F(NamenodeTest, mkdirDepth1) {
    std::string src = "/testing/test_mkdir";
    hadoop::hdfs::MkdirsRequestProto mkdir_req;
    hadoop::hdfs::MkdirsResponseProto mkdir_resp;
    mkdir_req.set_createparent(true);
    mkdir_req.set_src(src);
    ASSERT_EQ(client->mkdir(mkdir_req, mkdir_resp),
              zkclient::ZkNnClient::MkdirResponse::Ok);
    ASSERT_TRUE(mkdir_resp.result());
    ASSERT_TRUE(client->file_exists(src));
    LOG(DEBUG) << "Finished all asserts";
}

TEST_F(NamenodeTest, mkdirDepth100) {
    std::string src;
    for (int i = 0; i < 100; ++i) {
        src.append("/testing/test_mkdir");
    }
    hadoop::hdfs::MkdirsRequestProto mkdir_req;
    hadoop::hdfs::MkdirsResponseProto mkdir_resp;
    mkdir_req.set_createparent(true);
    mkdir_req.set_src(src);
    ASSERT_EQ(client->mkdir(mkdir_req, mkdir_resp),
              zkclient::ZkNnClient::MkdirResponse::Ok);
    ASSERT_TRUE(mkdir_resp.result());
    ASSERT_TRUE(client->file_exists(src));
}

TEST_F(NamenodeTest, mkdirExistentDirectory) {
    std::string src = "/testing/test_mkdir";
    hadoop::hdfs::MkdirsRequestProto mkdir_req;
    hadoop::hdfs::MkdirsResponseProto mkdir_resp;
    mkdir_req.set_createparent(true);
    mkdir_req.set_src(src);
    ASSERT_EQ(client->mkdir(mkdir_req, mkdir_resp),
              zkclient::ZkNnClient::MkdirResponse::Ok);
    ASSERT_TRUE(mkdir_resp.result());
    ASSERT_TRUE(client->file_exists(src));

    // Now create again.
    // TODO(Yufeng): should really introduce a new response enum
    ASSERT_EQ(client->mkdir(mkdir_req, mkdir_resp),
              zkclient::ZkNnClient::MkdirResponse::Ok);
    ASSERT_TRUE(mkdir_resp.result());
}

TEST_F(NamenodeTest, mkdirExistentFile) {
    // Create a file.
    std::string src = "/testing/test_mkdir_file";
    hadoop::hdfs::CreateRequestProto create_req = getCreateRequestProto(src);
    hadoop::hdfs::CreateResponseProto create_resp;
    ASSERT_EQ(client->create_file(create_req, create_resp),
              zkclient::ZkNnClient::CreateResponse::Ok);

    hadoop::hdfs::HdfsFileStatusProto file_status = create_resp.fs();
    ASSERT_EQ(file_status.filetype(),
              hadoop::hdfs::HdfsFileStatusProto::IS_FILE);
    ASSERT_TRUE(client->file_exists(src));

    // Now create a directory with the same name.
    src.insert(0, "/");
    hadoop::hdfs::MkdirsRequestProto mkdir_req;
    hadoop::hdfs::MkdirsResponseProto mkdir_resp;
    mkdir_req.set_createparent(true);
    mkdir_req.set_src(src);
    ASSERT_EQ(client->mkdir(mkdir_req, mkdir_resp),
              zkclient::ZkNnClient::MkdirResponse::FailedZnodeCreation);
    ASSERT_FALSE(mkdir_resp.result());
}

TEST_F(NamenodeTest, mkdirPerformance) {
  el::Loggers::setVerboseLevel(9);

  std::string root_src = "/testing";
  std::vector<int> depths = {1, 10, 50};
  std::vector<int> num_iters = {10, 100, 1000};

  // Benchmark depth 10.
  for (auto d : depths) {
    for (auto i : num_iters) {
      for (int j = 0; j < i; j++) {
        std::string curr_src = root_src;
        for (int k = 0; k < d; k++) {
          curr_src += "/test_mkdir_depth" + std::to_string(d) +
              "_num" + std::to_string(i) + "_iter" + std::to_string(j);
        }
        TIMED_SCOPE_IF(mkdirPerformanceTimer,
                       "mkdir_depth" + std::to_string(d), VLOG_IS_ON(9));
        hadoop::hdfs::MkdirsRequestProto mkdir_req;
        hadoop::hdfs::MkdirsResponseProto mkdir_resp;
        mkdir_req.set_createparent(true);
        mkdir_req.set_src(curr_src);
        ASSERT_EQ(client->mkdir(mkdir_req, mkdir_resp),
                  zkclient::ZkNnClient::MkdirResponse::Ok);
        ASSERT_TRUE(mkdir_resp.result());
      }
    }
  }
}
