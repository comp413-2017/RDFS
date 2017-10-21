// Copyright 2017 Rice University, COMP 413 2017

#include "NameNodeTest.h"

std::vector<std::string> createTestString(
        int num_files, int num_dirs
) {
    std::vector<std::string> generated_files;
    std::string prefix = std::to_string(num_files) + "_" +
        std::to_string(num_dirs) + "_";
    // Create the intermediate directories
    for (int i = 0; i < num_files; i++) {
        std::string temp = "";
        for (int d = 0; d < num_dirs; d++) {
            temp = temp + "/" + prefix + "f" + std::to_string(i) + "_d" +
                std::to_string(d);
        }
        // Now add the actual file
        temp = temp + "/" + prefix + "file";
        generated_files.push_back(temp);
    }
    return generated_files;
}

TEST_F(NamenodeTest, createBasicFile) {
    hadoop::hdfs::CreateRequestProto create_req =
        getCreateRequestProto("basic_file");
    hadoop::hdfs::CreateResponseProto create_resp;
    ASSERT_EQ(client->create_file(create_req, create_resp),
              zkclient::ZkNnClient::CreateResponse::Ok);

    hadoop::hdfs::HdfsFileStatusProto file_status = create_resp.fs();
    ASSERT_EQ(file_status.filetype(),
              hadoop::hdfs::HdfsFileStatusProto::IS_FILE);
    ASSERT_TRUE(client->file_exists("basic_file"));
}

TEST_F(NamenodeTest, createExistingFile) {
    // First, create a file.
    hadoop::hdfs::CreateRequestProto create_req =
        getCreateRequestProto("test_file");
    hadoop::hdfs::CreateResponseProto create_resp;
    ASSERT_EQ(client->create_file(create_req, create_resp),
              zkclient::ZkNnClient::CreateResponse::Ok);
    ASSERT_TRUE(client->file_exists("test_file"));

    // Then, try to create it again
    ASSERT_EQ(client->create_file(create_req, create_resp),
              zkclient::ZkNnClient::CreateResponse::FileAlreadyExists);
}

TEST_F(NamenodeTest, createChildFile) {
    // Create a file in a folder that doesn't already exist
    hadoop::hdfs::CreateRequestProto create_req =
        getCreateRequestProto("/test_dir/child_file");
    create_req.set_createparent(true);
    hadoop::hdfs::CreateResponseProto create_resp;
    ASSERT_EQ(client->create_file(create_req, create_resp),
              zkclient::ZkNnClient::CreateResponse::Ok);

    // Check to see that the folder was created
    hadoop::hdfs::GetListingRequestProto listing_req;
    hadoop::hdfs::GetListingResponseProto listing_resp;
    zkclient::ZkNnClient::ListingResponse client_response;
    listing_req.set_src("/test_dir");
    listing_req.set_startafter(0);
    listing_req.set_needlocation(false);
    client_response = client->get_listing(listing_req, listing_resp);
    ASSERT_EQ(client_response, zkclient::ZkNnClient::ListingResponse::Ok)
                  << "File not found";

    // Check that we've gotten exactly the file we expected.
    hadoop::hdfs::DirectoryListingProto dir_listing;
    hadoop::hdfs::HdfsFileStatusProto file_status;
    dir_listing = listing_resp.dirlist();
    EXPECT_EQ(dir_listing.partiallisting_size(), 1);
    file_status = dir_listing.partiallisting(0);
    ASSERT_EQ(file_status.filetype(),
              hadoop::hdfs::HdfsFileStatusProto::IS_FILE);
    ASSERT_EQ(file_status.path(), "/test_dir/child_file");
}

TEST_F(NamenodeTest, createChildFile2) {
    // Create the folder
    hadoop::hdfs::MkdirsRequestProto mkdir_req;
    hadoop::hdfs::MkdirsResponseProto mkdir_resp;
    mkdir_req.set_src("/existing_dir");
    mkdir_req.set_createparent(false);
    client->mkdir(mkdir_req, mkdir_resp);
    ASSERT_TRUE(mkdir_resp.result());

    // Check to see that the folder was created
    hadoop::hdfs::GetListingRequestProto listing_req;
    hadoop::hdfs::GetListingResponseProto listing_resp;
    zkclient::ZkNnClient::ListingResponse client_response;
    listing_req.set_src("/existing_dir");
    listing_req.set_startafter(0);
    listing_req.set_needlocation(false);
    client_response = client->get_listing(listing_req, listing_resp);
    ASSERT_EQ(client_response, zkclient::ZkNnClient::ListingResponse::Ok)
                  << "File not found";

    // Create a file in a folder that already exists
    hadoop::hdfs::CreateRequestProto create_req =
        getCreateRequestProto("/existing_dir/child_file");
    create_req.set_createparent(true);
    hadoop::hdfs::CreateResponseProto create_resp;
    ASSERT_EQ(client->create_file(create_req, create_resp),
              zkclient::ZkNnClient::CreateResponse::Ok);

    // Check that we've gotten exactly the file we expected.
    client_response = client->get_listing(listing_req, listing_resp);
    hadoop::hdfs::DirectoryListingProto dir_listing;
    hadoop::hdfs::HdfsFileStatusProto file_status;
    dir_listing = listing_resp.dirlist();
    EXPECT_EQ(dir_listing.partiallisting_size(), 1);
    file_status = dir_listing.partiallisting(0);
    ASSERT_EQ(file_status.filetype(),
              hadoop::hdfs::HdfsFileStatusProto::IS_FILE);
    ASSERT_EQ(file_status.path(), "/existing_dir/child_file");
}

TEST_F(NamenodeTest, creationPerformance) {
    el::Loggers::setVerboseLevel(9);
    std::vector<std::string> files;
    hadoop::hdfs::CreateRequestProto create_req;
    hadoop::hdfs::CreateResponseProto create_resp;
    std::vector<int> num_files = {5, 10, 100};
    std::vector<int> num_dirs = {10, 100};

    for (int f : num_files) {
        for (int d : num_dirs) {
            files = createTestString(f, d);
            TIMED_SCOPE_IF(timerObj1, "create " + std::to_string(f) +
                " files with " + std::to_string(d) +
                " intermediate directories", VLOG_IS_ON(9));
            for (std::string f : files) {
                create_req = getCreateRequestProto(f);
                create_req.set_createparent(true);
                ASSERT_EQ(client->create_file(create_req, create_resp),
                          zkclient::ZkNnClient::CreateResponse::Ok);
            }
        }
    }
}

