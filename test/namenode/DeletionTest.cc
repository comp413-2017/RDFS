// Copyright 2017 Rice University, COMP 413 2017

#include <limits>
#include "NameNodeTest.h"

TEST_F(NamenodeTest, deleteBasicFile) {
    // Create a file.
    std::string src = "delete_file";
    hadoop::hdfs::CreateRequestProto create_req = getCreateRequestProto(src);
    hadoop::hdfs::CreateResponseProto create_resp;
    ASSERT_TRUE(client->create_file(create_req, create_resp));

    // Must give zookeeper time to construct file
    sleep(20);

    // Delete the file
    hadoop::hdfs::DeleteRequestProto delete_req;
    delete_req.set_recursive(false);
    delete_req.set_src(src);
    hadoop::hdfs::DeleteResponseProto delete_resp;
    ASSERT_EQ(client->destroy(delete_req, delete_resp), zkclient::ZkNnClient::DeleteResponse::Ok);
    ASSERT_FALSE(client->file_exists(src));
}

TEST_F(NamenodeTest, deleteEmptyDirectory) {
    // Create an empty directory
    std::string src = "/basic_dir";
    hadoop::hdfs::MkdirsRequestProto mkdir_req;
    hadoop::hdfs::MkdirsResponseProto mkdir_resp;
    hadoop::hdfs::FsPermissionProto permission;
    permission.set_perm(std::numeric_limits<uint32_t>::max()); // Max permission.
    mkdir_req.set_src(src);
    mkdir_req.set_createparent(true);
    mkdir_req.set_allocated_masked(&permission);
    ASSERT_EQ(client->mkdir(mkdir_req, mkdir_resp), zkclient::ZkNnClient::MkdirResponse::Ok);

    // Must give zookeeper time to construct file
    sleep(10);

    // Delete the directory
    hadoop::hdfs::DeleteRequestProto delete_req;
    delete_req.set_recursive(true);
    delete_req.set_src(src);
    hadoop::hdfs::DeleteResponseProto delete_resp;
    ASSERT_EQ(client->destroy(delete_req, delete_resp), zkclient::ZkNnClient::DeleteResponse::Ok);
    ASSERT_FALSE(client->file_exists(src));
}

TEST_F(NamenodeTest, deleteDirectory) {
    // Create an empty directory
    std::string src_dir = "/test_dir";
    std::string src = "/test_dir/child_file";
    hadoop::hdfs::CreateRequestProto create_req = getCreateRequestProto(src);
    create_req.set_createparent(true);
    hadoop::hdfs::CreateResponseProto create_resp;
    ASSERT_TRUE(client->create_file(create_req, create_resp));

    // Must give zookeeper time to construct file
    sleep(10);

    // Delete the directory
    hadoop::hdfs::DeleteRequestProto delete_req;
    delete_req.set_recursive(true);
    delete_req.set_src(src_dir);
    hadoop::hdfs::DeleteResponseProto delete_resp;
    ASSERT_EQ(client->destroy(delete_req, delete_resp), zkclient::ZkNnClient::DeleteResponse::Ok);
    ASSERT_FALSE(client->file_exists(src_dir));
    ASSERT_FALSE(client->file_exists(src));

}

TEST_F(NamenodeTest, deleteNonexistantFile) {
    // Make up filename
    std::string src = "delete_file";
    ASSERT_FALSE(client->file_exists(src));

    // Try and delete it
    hadoop::hdfs::DeleteRequestProto delete_req;
    delete_req.set_recursive(false);
    delete_req.set_src(src);
    hadoop::hdfs::DeleteResponseProto delete_resp;
    ASSERT_EQ(client->destroy(delete_req, delete_resp), zkclient::ZkNnClient::DeleteResponse::FileDoesNotExist);
}

TEST_F(NamenodeTest, deleteNestedNonexistantFile) {
    // Create an empty directory
    std::string src_dir = "/basic_dir";
    hadoop::hdfs::MkdirsRequestProto mkdir_req;
    hadoop::hdfs::MkdirsResponseProto mkdir_resp;
    hadoop::hdfs::FsPermissionProto permission;
    permission.set_perm(std::numeric_limits<uint32_t>::max()); // Max permission.
    mkdir_req.set_src(src_dir);
    mkdir_req.set_createparent(true);
    mkdir_req.set_allocated_masked(&permission);
    ASSERT_EQ(client->mkdir(mkdir_req, mkdir_resp), zkclient::ZkNnClient::MkdirResponse::Ok);

    // Must give zookeeper time to construct file
    sleep(10);

    // Make up filename
    std::string src = "/basic_dir/basic_file";
    ASSERT_FALSE(client->file_exists(src));
    hadoop::hdfs::DeleteRequestProto delete_req;
    delete_req.set_recursive(true);
    delete_req.set_src(src);
    hadoop::hdfs::DeleteResponseProto delete_resp;
    ASSERT_EQ(client->destroy(delete_req, delete_resp), zkclient::ZkNnClient::DeleteResponse::FileDoesNotExist);

}