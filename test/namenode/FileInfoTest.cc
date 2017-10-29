// Copyright 2017 Rice University, COMP 413 2017

#include "NameNodeTest.h"


TEST_F(NamenodeTest, statFile) {
  auto src = std::string{"/testing/stat_test_file"};
  hadoop::hdfs::CreateRequestProto create_req;
  hadoop::hdfs::CreateResponseProto create_resp;
  create_req.set_src(src);
  create_req.set_clientname("test_client_name");
  create_req.set_createparent(true);
  create_req.set_blocksize(0);
  create_req.set_replication(1);
  create_req.set_createflag(0);
  ASSERT_EQ(client->create_file(create_req, create_resp),
            zkclient::ZkNnClient::CreateResponse::Ok);

  hadoop::hdfs::GetFileInfoRequestProto file_info_req;
  hadoop::hdfs::GetFileInfoResponseProto file_info_resp;
  file_info_req.set_src(src);
  ASSERT_EQ(client->get_info(file_info_req, file_info_resp),
            zkclient::ZkNnClient::GetFileInfoResponse::Ok);
  ASSERT_TRUE(file_info_resp.has_fs());
  auto fs = file_info_resp.fs();
  ASSERT_EQ(0, fs.length());
  ASSERT_EQ(0, fs.blocksize());
}

TEST_F(NamenodeTest, statDir) {
  auto src = std::string{"/testing/stat_test_dir"};
  hadoop::hdfs::MkdirsRequestProto mkdir_req;
  hadoop::hdfs::MkdirsResponseProto mkdir_resp;
  mkdir_req.set_createparent(true);
  mkdir_req.set_src(src);
  ASSERT_EQ(client->mkdir(mkdir_req, mkdir_resp),
            zkclient::ZkNnClient::MkdirResponse::Ok);
  ASSERT_TRUE(mkdir_resp.result());
  ASSERT_TRUE(client->file_exists(src));

  hadoop::hdfs::GetFileInfoRequestProto file_info_req;
  hadoop::hdfs::GetFileInfoResponseProto file_info_resp;
  file_info_req.set_src(src);
  ASSERT_EQ(client->get_info(file_info_req, file_info_resp),
            zkclient::ZkNnClient::GetFileInfoResponse::Ok);
}

TEST_F(NamenodeTest, statNonExistentFile) {
  auto src = std::string{"/testing/stat_no_file"};
  hadoop::hdfs::GetFileInfoRequestProto file_info_req;
  hadoop::hdfs::GetFileInfoResponseProto file_info_resp;
  file_info_req.set_src(src);
  ASSERT_EQ(client->get_info(file_info_req, file_info_resp),
            zkclient::ZkNnClient::GetFileInfoResponse::FileDoesNotExist);
}

TEST_F(NamenodeTest, statPerformance) {
  el::Loggers::setVerboseLevel(9);

  auto root_src = std::string{"/testing"};
  auto depths = std::vector<int>{5, 100};
  auto num_chars_per_level = std::vector<int>{5, 50};
  auto num_iters = std::vector<int>{10, 100, 1000};
  auto pathnames = std::map<int, std::map<int, std::string>>{};

  // Create the necessary files.
  for (auto d : depths) {
    for (auto c : num_chars_per_level) {
      auto path = std::string{root_src};
      auto level = std::string{"/"};
      for (int i = 0; i < c; i++) {
        level += "s";
      }
      for (int j = 0; j < d; j++) {
        path += level;
      }
      pathnames[d][c] = path;

      hadoop::hdfs::CreateRequestProto create_req;
      hadoop::hdfs::CreateResponseProto create_resp;
      create_req.set_src(path);
      create_req.set_clientname("test_client_name");
      create_req.set_createparent(true);
      create_req.set_blocksize(0);
      create_req.set_replication(1);
      create_req.set_createflag(0);
      ASSERT_EQ(client->create_file(create_req, create_resp),
                zkclient::ZkNnClient::CreateResponse::Ok);
    }
  }

  // Now the actual benchmarking.
  for (auto d : depths) {
    for (auto c : num_chars_per_level) {
      for (auto i : num_iters) {
        for (int j = 0; j < i; j++) {
          TIMED_SCOPE_IF(statPerformanceTimer, "stat depth " +
              std::to_string(d) + " num_chars_per_level " + std::to_string(c),
                         VLOG_IS_ON(9));
          hadoop::hdfs::GetFileInfoRequestProto file_info_req;
          hadoop::hdfs::GetFileInfoResponseProto file_info_resp;
          file_info_req.set_src(pathnames[d][c]);
          ASSERT_EQ(client->get_info(file_info_req, file_info_resp),
                    zkclient::ZkNnClient::GetFileInfoResponse::Ok);
        }
      }
    }
  }
}
