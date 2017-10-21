// Copyright 2017 Rice University, COMP 413 2017

#include <limits>
#include "NameNodeTest.h"

TEST_F(NamenodeTest, deleteBasicFile) {
    // Create a file.
    std::string src = "delete_file";
    hadoop::hdfs::CreateRequestProto create_req = getCreateRequestProto(src);
    hadoop::hdfs::CreateResponseProto create_resp;
    ASSERT_EQ(client->create_file(create_req, create_resp),
              zkclient::ZkNnClient::CreateResponse::Ok);

    // Try and delete before completing
    hadoop::hdfs::DeleteRequestProto delete_req;
    hadoop::hdfs::DeleteResponseProto delete_resp;
    delete_req.set_src(src);
    delete_req.set_recursive(false);
    ASSERT_EQ(client->destroy(delete_req, delete_resp),
              zkclient::ZkNnClient::DeleteResponse::FileUnderConstruction);
    ASSERT_TRUE(client->file_exists(src));

    hadoop::hdfs::CompleteRequestProto complete_req;
    hadoop::hdfs::CompleteResponseProto complete_resp;
    complete_req.set_src(src);
    client->complete(complete_req, complete_resp);

    // Delete the file
    hadoop::hdfs::DeleteRequestProto delete_req2;
    delete_req2.set_recursive(false);
    delete_req2.set_src(src);
    hadoop::hdfs::DeleteResponseProto delete_resp2;
    ASSERT_EQ(client->destroy(delete_req2, delete_resp2),
              zkclient::ZkNnClient::DeleteResponse::Ok);
    ASSERT_FALSE(client->file_exists(src));
}

TEST_F(NamenodeTest, deleteEmptyDirectory) {
    // Create a directory.
    std::string src = "/test_empty_dir";
    hadoop::hdfs::MkdirsRequestProto mkdir_req;
    hadoop::hdfs::MkdirsResponseProto mkdir_resp;
    mkdir_req.set_src(src);
    mkdir_req.set_createparent(false);
    client->mkdir(mkdir_req, mkdir_resp);
    ASSERT_TRUE(mkdir_resp.result());

    // Try and delete the directory, without recursive
    hadoop::hdfs::DeleteRequestProto delete_req;
    hadoop::hdfs::DeleteResponseProto delete_resp;
    delete_req.set_src(src);
    delete_req.set_recursive(false);
    ASSERT_EQ(client->destroy(delete_req, delete_resp),
              zkclient::ZkNnClient::DeleteResponse::FileIsDirectoryMismatch);
    ASSERT_TRUE(client->file_exists(src));

    // Try and delete the directory, with recursive
    hadoop::hdfs::DeleteRequestProto delete_req2;
    hadoop::hdfs::DeleteResponseProto delete_resp2;
    delete_req2.set_src(src);
    delete_req2.set_recursive(true);
    ASSERT_EQ(client->destroy(delete_req2, delete_resp2),
              zkclient::ZkNnClient::DeleteResponse::Ok);
    ASSERT_FALSE(client->file_exists(src));
}

TEST_F(NamenodeTest, deleteDirectory) {
    // Create an empty directory
    std::string src_dir = "/test_delete_dir";
    std::string src = "/test_delete_dir/child_file";
    hadoop::hdfs::CreateRequestProto create_req = getCreateRequestProto(src);
    create_req.set_createparent(true);
    hadoop::hdfs::CreateResponseProto create_resp;
    ASSERT_EQ(client->create_file(create_req, create_resp),
              zkclient::ZkNnClient::CreateResponse::Ok);

    hadoop::hdfs::CompleteRequestProto complete_req;
    hadoop::hdfs::CompleteResponseProto complete_resp;
    complete_req.set_src(src);
    client->complete(complete_req, complete_resp);

    // Delete the directory
    hadoop::hdfs::DeleteRequestProto delete_req;
    delete_req.set_recursive(true);
    delete_req.set_src(src_dir);
    hadoop::hdfs::DeleteResponseProto delete_resp;
    ASSERT_EQ(client->destroy(delete_req, delete_resp),
              zkclient::ZkNnClient::DeleteResponse::Ok);
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
    ASSERT_EQ(client->destroy(delete_req, delete_resp),
              zkclient::ZkNnClient::DeleteResponse::FileDoesNotExist);
}

TEST_F(NamenodeTest, deleteNestedNonexistantFile) {
    // Create an empty directory
    std::string src_dir = "basic_dir";
    hadoop::hdfs::MkdirsRequestProto mkdir_req;
    hadoop::hdfs::MkdirsResponseProto mkdir_resp;
    mkdir_req.set_src(src_dir);
    mkdir_req.set_createparent(true);
    ASSERT_EQ(client->mkdir(mkdir_req, mkdir_resp),
              zkclient::ZkNnClient::MkdirResponse::Ok);

    // Must give zookeeper time to construct file
    sleep(10);

    // Make up filename
    std::string src = "basic_dir/basic_file";
    ASSERT_FALSE(client->file_exists(src));
    hadoop::hdfs::DeleteRequestProto delete_req;
    delete_req.set_recursive(true);
    delete_req.set_src(src);
    hadoop::hdfs::DeleteResponseProto delete_resp;
    ASSERT_EQ(client->destroy(delete_req, delete_resp),
              zkclient::ZkNnClient::DeleteResponse::FileDoesNotExist);
}

TEST_F(NamenodeTest, deleteBasicFileWithBlock) {
    int error;
    std::string src = "delete_file";
    hadoop::hdfs::CreateRequestProto create_req = getCreateRequestProto(src);
    hadoop::hdfs::CreateResponseProto create_resp;
    ASSERT_EQ(client->create_file(create_req, create_resp),
              zkclient::ZkNnClient::CreateResponse::Ok);
    std::uint64_t block_id = 1234;
    std::vector<std::uint8_t> block_vec(sizeof(std::uint64_t));
    memcpy(block_vec.data(), &block_id, sizeof(std::uint64_t));
    ASSERT_TRUE(zk->create("/fileSystem/delete_file/block-0000000000",
                           block_vec,
                           error));
    ASSERT_TRUE(zk->create("/block_locations/1234",
                           ZKWrapper::EMPTY_VECTOR,
                           error));

    // TODO(2016): create real block_locations for this block once we start
    // doing complete legitimately

    hadoop::hdfs::CompleteRequestProto complete_req;
    hadoop::hdfs::CompleteResponseProto complete_resp;
    complete_req.set_src(src);
    client->complete(complete_req, complete_resp);
    ASSERT_TRUE(complete_resp.result());

    hadoop::hdfs::DeleteRequestProto delete_req;
    hadoop::hdfs::DeleteResponseProto delete_resp;
    delete_req.set_src(src);
    delete_req.set_recursive(false);
    ASSERT_EQ(client->destroy(delete_req, delete_resp),
              zkclient::ZkNnClient::DeleteResponse::Ok);
    ASSERT_FALSE(client->file_exists(src));
}

TEST_F(NamenodeTest, deleteBreadthPerformance) {
    el::Loggers::setVerboseLevel(9);

    int i = 0;
    std::vector<int> depths = {5, 10, 100};
    std::vector<int> dir_nums = {10, 100, 1000};

    for (auto depth : depths) {
        for (auto dir_num : dir_nums) {
            auto dir_string = "/testing/delete_testing_depth" +
                std::to_string(depth) +
                "_size" + std::to_string(dir_num) + "/";

            for (i = 0; i < depth - 2; i++) {
                dir_string += std::to_string(i) + "/";
            }

            for (i = 0; i < dir_num; i++) {
                hadoop::hdfs::CreateRequestProto create_req;
                hadoop::hdfs::CreateResponseProto create_resp;
                create_req.set_src(dir_string + std::to_string(i));
                create_req.set_createparent(true);
                EXPECT_EQ(client->create_file(create_req, create_resp),
                          zkclient::ZkNnClient::CreateResponse::Ok);

                hadoop::hdfs::CompleteRequestProto complete_req;
                hadoop::hdfs::CompleteResponseProto complete_resp;
                complete_req.set_src(dir_string + std::to_string(i));
                client->complete(complete_req, complete_resp);
            }
        }
    }

    // Tests delete directory with 10, 100, 1000 entries

    for (auto depth : depths) {
        for (auto dir_num : dir_nums) {
            auto timer_string = "delete_depth" +
                    std::to_string(depth) +
                    "_size" + std::to_string(dir_num);

            auto dir_string = "/testing/delete_testing_depth" +
                std::to_string(depth) +
                "_size" + std::to_string(dir_num) + "/";

            for (i = 0; i < depth - 2; i++) {
                dir_string += std::to_string(i) + "/";
            }

            TIMED_SCOPE_IF(deletionPerformanceObj,
                    timer_string,
                    VLOG_IS_ON(9));

            hadoop::hdfs::DeleteRequestProto delete_req;
            delete_req.set_recursive(true);
            delete_req.set_src(dir_string);
            hadoop::hdfs::DeleteResponseProto delete_resp;
            ASSERT_EQ(client->destroy(delete_req, delete_resp),
                      zkclient::ZkNnClient::DeleteResponse::Ok);
            ASSERT_FALSE(client->file_exists(dir_string));
        }
    }
}

TEST_F(NamenodeTest, deleteDepthPerformance) {
    el::Loggers::setVerboseLevel(9);

    int i = 0;
    std::string child = "child_file";
    std::vector<int> depths = {5, 10, 100};

    for (auto depth : depths) {
        auto dir_string = "/testing/delete_testing_depth" +
            std::to_string(depth) + "/";

        for (i = 0; i < depth - 2; i++) {
            dir_string += std::to_string(i) + "/";
        }

        hadoop::hdfs::CreateRequestProto create_req;
        hadoop::hdfs::CreateResponseProto create_resp;
        create_req.set_src(dir_string + child);
        create_req.set_createparent(true);
        EXPECT_EQ(client->create_file(create_req, create_resp),
                  zkclient::ZkNnClient::CreateResponse::Ok);

        hadoop::hdfs::CompleteRequestProto complete_req;
        hadoop::hdfs::CompleteResponseProto complete_resp;
        complete_req.set_src(dir_string + child);
        client->complete(complete_req, complete_resp);
    }

    // Tests delete directory with 10, 100, 1000 entries

    for (auto depth : depths) {
        auto timer_string = "delete_depth" +
                            std::to_string(depth);

        auto dir_string = "/testing/delete_testing_depth" +
            std::to_string(depth) + "/";

        TIMED_SCOPE_IF(deletionPerformanceObj,
                       timer_string,
                       VLOG_IS_ON(9));

        hadoop::hdfs::DeleteRequestProto delete_req;
        delete_req.set_recursive(true);
        delete_req.set_src(dir_string);
        hadoop::hdfs::DeleteResponseProto delete_resp;
        ASSERT_EQ(client->destroy(delete_req, delete_resp),
                  zkclient::ZkNnClient::DeleteResponse::Ok);
        ASSERT_FALSE(client->file_exists(dir_string));
    }
}
