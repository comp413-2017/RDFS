// Copyright 2017 Rice University, COMP 413 2017

#include "NameNodeTest.h"

std::vector<std::string> renameTestString(
        int depth, int num_entries
) {
    std::vector<std::string> generated_files;
    std::string prefix = std::to_string(depth) + "_" +
                         std::to_string(num_entries);
    // Create the intermediate directories
    for (int i = 0; i < num_entries; i++) {
        std::string temp = "";
        for (int d = 0; d < depth; d++) {
            temp = temp + "/" + prefix + "_d" + std::to_string(d);
        }
        // Now add the actual file
        temp = temp + "/" + prefix + "entry" + std::to_string(i);
        generated_files.push_back(temp);
    }
    return generated_files;
}

TEST_F(NamenodeTest, renameBasicFile) {
    int error_code;

    // Create a test file for renaming
    hadoop::hdfs::CreateRequestProto create_req =
            getCreateRequestProto("orig_file");
    create_req.set_blocksize(0);
    create_req.set_replication(1);
    create_req.set_createflag(0);
    hadoop::hdfs::CreateResponseProto create_resp;
    ASSERT_EQ(client->create_file(create_req, create_resp),
    zkclient::ZkNnClient::CreateResponse::Ok);

    // Create a child of the old file with a fake block
    std::string new_path;
    zk->create_sequential("/fileSystem/orig_file/block-",
    zk->get_byte_vector("Block uuid"), new_path, false, error_code);
    ASSERT_EQ(0, error_code);
    ASSERT_EQ("/fileSystem/orig_file/block-0000000000", new_path);

    // Rename
    hadoop::hdfs::RenameRequestProto rename_req;
    hadoop::hdfs::RenameResponseProto rename_resp;
    rename_req.set_src("/orig_file");
    rename_req.set_dst("/renamed_file");
    ASSERT_EQ(client->rename(rename_req, rename_resp),
    zkclient::ZkNnClient::RenameResponse::Ok);

    // Ensure that the renamed node has the same data
    zkclient::FileZNode renamed_data;
    std::vector<std::uint8_t> data(sizeof(renamed_data));
    ASSERT_TRUE(zk->get("/fileSystem/renamed_file", data, error_code));
    std::uint8_t *buffer = &data[0];
    memcpy(&renamed_data, buffer, sizeof(renamed_data));
    ASSERT_EQ(1, renamed_data.replication);
    ASSERT_EQ(0, renamed_data.blocksize);
    ASSERT_EQ(2, renamed_data.filetype);

    // Ensure that the file's child indicating block_id was renamed as well
    auto new_block_data = std::vector<std::uint8_t>();
    zk->get("/fileSystem/renamed_file/block-0000000000",
    new_block_data, error_code);
    ASSERT_EQ(0, error_code);
    ASSERT_EQ("Block uuid",
    std::string(new_block_data.begin(), new_block_data.end()));

    // Ensure that old_name was delete
    bool exist;
    zk->exists("/fileSystem/orig_file", exist, error_code);
    ASSERT_EQ(false, exist);
}

TEST_F(NamenodeTest, renameNonExistingFile) {
    //// Rename
    hadoop::hdfs::RenameRequestProto rename_req;
    hadoop::hdfs::RenameResponseProto rename_resp;
    rename_req.set_src("/blank_file");
    rename_req.set_dst("/new_file");
    client->rename(rename_req, rename_resp);
    ASSERT_EQ(client->rename(rename_req, rename_resp),
    zkclient::ZkNnClient::RenameResponse::FileDoesNotExist);
}

TEST_F(NamenodeTest, movingPerformance) {
    el::Loggers::setVerboseLevel(9);
    std::vector<std::string> files;
    hadoop::hdfs::CreateRequestProto create_req;
    hadoop::hdfs::CreateResponseProto create_resp;
    hadoop::hdfs::RenameRequestProto rename_req;
    hadoop::hdfs::RenameResponseProto rename_resp;
    std::vector<int> depths = {5, 10, 100};
    std::vector<int> entries = {10, 100, 1000};
    std::vector<int> requests = {10, 100, 1000};

    for (int d : depths) {
        for (int e : entries) {
            files = renameTestString(d, e);
            for (std::string f : files) {
                create_req = getCreateRequestProto(f);
                create_req.set_createparent(true);
                ASSERT_EQ(client->create_file(create_req, create_resp),
                zkclient::ZkNnClient::CreateResponse::Ok);
            }
            // Rename
            std::string top_level = "/" + std::to_string(d) + "_" +
                                    std::to_string(e) + "_d0";
            for (int total_requests : requests) {
                TIMED_SCOPE_IF(timerObj1, "renaming " +
                std::to_string(total_requests) + " times with depth " +
                std::to_string(d) + " and " + std::to_string(e) +
                " entries.", VLOG_IS_ON(9));
                for (int r; r < total_requests; r++) {
                    std::string renamed = "/renamed" + std::to_string(r);
                    rename_req.set_src(top_level);
                    rename_req.set_dst(renamed);
                    ASSERT_EQ(client->rename(rename_req, rename_resp),
                    zkclient::ZkNnClient::RenameResponse::Ok);
                    top_level = renamed;
                }
            }
        }
    }
}
