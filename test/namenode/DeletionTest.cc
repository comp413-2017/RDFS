// Copyright 2017 Rice University, COMP 413 2017

#include <limits>
#include "NameNodeTest.h"

TEST_F(NamenodeTest, deleteBasicFile) {
    // Create a file.
    std::string src = "/testing/delete_file";
    hadoop::hdfs::CreateRequestProto create_req = getCreateRequestProto(src);
    hadoop::hdfs::CreateResponseProto create_resp;
    ASSERT_EQ(client->create_file(create_req, create_resp), zkclient::ZkNnClient::CreateResponse::Ok);

    zkclient::FileZNode znode_data;
    client->read_file_znode(znode_data, src);

    while(znode_data.under_construction == zkclient::FileStatus::UnderConstruction) {
        sleep(10);
        client->read_file_znode(znode_data, src);
    }

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
    std::string src = "/testing/basic_dir";
    hadoop::hdfs::MkdirsRequestProto mkdir_req;
    hadoop::hdfs::MkdirsResponseProto mkdir_resp;
    hadoop::hdfs::FsPermissionProto permission;
    permission.set_perm(std::numeric_limits<uint32_t>::max()); // Max permission.
    mkdir_req.set_src(src);
    mkdir_req.set_createparent(true);
    mkdir_req.set_allocated_masked(&permission);
    ASSERT_EQ(client->mkdir(mkdir_req, mkdir_resp), zkclient::ZkNnClient::MkdirResponse::Ok);

    zkclient::FileZNode znode_data;
    client->read_file_znode(znode_data, src);

    while(znode_data.under_construction == zkclient::FileStatus::UnderConstruction) {
        sleep(10);
        client->read_file_znode(znode_data, src);
    }

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
    std::string src_dir = "/testing/test_dir";
    std::string src = "/testing/test_dir/child_file";
    hadoop::hdfs::CreateRequestProto create_req = getCreateRequestProto(src);
    create_req.set_createparent(true);
    hadoop::hdfs::CreateResponseProto create_resp;
    ASSERT_EQ(client->create_file(create_req, create_resp), zkclient::ZkNnClient::CreateResponse::Ok);

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
    std::string src = "/testing/delete_file";
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
    std::string src_dir = "/testing/basic_dir";
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
    std::string src = "/testing/basic_dir/basic_file";
    ASSERT_FALSE(client->file_exists(src));
    hadoop::hdfs::DeleteRequestProto delete_req;
    delete_req.set_recursive(true);
    delete_req.set_src(src);
    hadoop::hdfs::DeleteResponseProto delete_resp;
    ASSERT_EQ(client->destroy(delete_req, delete_resp), zkclient::ZkNnClient::DeleteResponse::FileDoesNotExist);

}

TEST_F(NamenodeTest, deletionPerformance) {
    el::Loggers::setVerboseLevel(9);

    int i = 0;
    std::vector<int> depths = {5, 100};
    std::vector<int> dir_nums = {10, 100, 1000};

    for (auto depth : depths) {
        for (auto dir_num : dir_nums) {

            auto dir_string = "/testing/delete_testing_depth" + std::to_string(depth) +
                  "_size" + std::to_string(dir_num) + "/";

            for (i = 0; i < depth - 2; i++) {
                dir_string += std::to_string(i) + "/";
            }

            for (i = 0; i < dir_num; i++) {
                hadoop::hdfs::CreateRequestProto create_req;
                hadoop::hdfs::CreateResponseProto create_resp;
                create_req.set_src(dir_string + std::to_string(i));
                create_req.set_createparent(true);
                EXPECT_EQ(client->create_file(create_req, create_resp), zkclient::ZkNnClient::CreateResponse::Ok);
            }
        }
    }

    for (auto depth : depths) {
        for (auto dir_num : dir_nums) {
            auto timer_string = "delete_depth" +
                    std::to_string(depth) +
                    "_size" + std::to_string(dir_num);

            auto dir_string = "/testing/delete_testing_depth" + std::to_string(depth) + \
                    "_size" + std::to_string(dir_num) + "/";

            for (i = 0; i < depth - 2; i++) {
                dir_string += std::to_string(i) + "/";
            }

            TIMED_SCOPE_IF(listingPerformanceObj,
                    timer_string,
                    VLOG_IS_ON(9));

            hadoop::hdfs::DeleteRequestProto delete_req;
            delete_req.set_recursive(true);
            delete_req.set_src(dir_string);
            hadoop::hdfs::DeleteResponseProto delete_resp;
            ASSERT_EQ(client->destroy(delete_req, delete_resp), zkclient::ZkNnClient::DeleteResponse::Ok);
            ASSERT_FALSE(client->file_exists(dir_string));
        }
    }
}