#include <limits>
#include "NameNodeTest.h"


TEST_F(NamenodeTest, mkdirDepth1) {
    std::string src = "/test_mkdir";
    hadoop::hdfs::MkdirsRequestProto mkdir_req;
    hadoop::hdfs::MkdirsResponseProto mkdir_resp;
    hadoop::hdfs::FsPermissionProto permission;
    permission.set_perm(std::numeric_limits<uint32_t>::max()); // Max permission.
    mkdir_req.set_createparent(true);
    mkdir_req.set_src(src);
    mkdir_req.set_allocated_masked(&permission);
    ASSERT_EQ(client->mkdir(mkdir_req, mkdir_resp), zkclient::ZkNnClient::MkdirResponse::Ok);
    ASSERT_TRUE(mkdir_resp.result());
    ASSERT_TRUE(client->file_exists(src));
}

TEST_F(NamenodeTest, mkdirDepth1024) {
    std::string src;
    for (int i = 0; i < 1024; ++i) {
        src.append("/test_mkdir");
    }
    hadoop::hdfs::MkdirsRequestProto mkdir_req;
    hadoop::hdfs::MkdirsResponseProto mkdir_resp;
    hadoop::hdfs::FsPermissionProto permission;
    permission.set_perm(std::numeric_limits<uint32_t>::max()); // Max permission.
    mkdir_req.set_createparent(true);
    mkdir_req.set_src(src);
    mkdir_req.set_allocated_masked(&permission);
    ASSERT_EQ(client->mkdir(mkdir_req, mkdir_resp), zkclient::ZkNnClient::MkdirResponse::Ok);
    ASSERT_TRUE(mkdir_resp.result());
    ASSERT_TRUE(client->file_exists(src));
}

TEST_F(NamenodeTest, mkdirExistentDirectory) {
    std::string src = "/test_mkdir";
    hadoop::hdfs::MkdirsRequestProto mkdir_req;
    hadoop::hdfs::MkdirsResponseProto mkdir_resp;
    hadoop::hdfs::FsPermissionProto permission;
    permission.set_perm(std::numeric_limits<uint32_t>::max()); // Max permission.
    mkdir_req.set_createparent(true);
    mkdir_req.set_src(src);
    mkdir_req.set_allocated_masked(&permission);
    ASSERT_EQ(client->mkdir(mkdir_req, mkdir_resp), zkclient::ZkNnClient::MkdirResponse::Ok);
    ASSERT_TRUE(mkdir_resp.result());
    ASSERT_TRUE(client->file_exists(src));

    // Now create again.
    // TODO should really introduce a new response enum
    ASSERT_EQ(client->mkdir(mkdir_req, mkdir_resp), zkclient::ZkNnClient::MkdirResponse::Ok);
    ASSERT_TRUE(mkdir_resp.result());
}

TEST_F(NamenodeTest, mkdirExistentFile) {
    // Create a file.
    std::string src = "test_mkdir_file";
    hadoop::hdfs::CreateRequestProto create_req = getCreateRequestProto(src);
    hadoop::hdfs::CreateResponseProto create_resp;
    ASSERT_TRUE(client->create_file(create_req, create_resp));

    hadoop::hdfs::HdfsFileStatusProto file_status = create_resp.fs();
    ASSERT_EQ(file_status.filetype(), hadoop::hdfs::HdfsFileStatusProto::IS_FILE);
    ASSERT_TRUE(client->file_exists(src));

    // Now create a directory with the same name.
    src.insert(0, "/");
    hadoop::hdfs::MkdirsRequestProto mkdir_req;
    hadoop::hdfs::MkdirsResponseProto mkdir_resp;
    hadoop::hdfs::FsPermissionProto permission;
    permission.set_perm(std::numeric_limits<uint32_t>::max()); // Max permission.
    mkdir_req.set_createparent(true);
    mkdir_req.set_src(src);
    mkdir_req.set_allocated_masked(&permission);
    // TODO introduce a new response enum
    ASSERT_EQ(client->mkdir(mkdir_req, mkdir_resp), zkclient::ZkNnClient::MkdirResponse::Ok);
    ASSERT_FALSE(mkdir_resp.result());
}
