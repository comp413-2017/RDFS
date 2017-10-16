// Copyright 2017 Rice University, COMP 413 2017

#include "NameNodeTest.h"

TEST_F(NamenodeTest, createBasicFile) {
    hadoop::hdfs::CreateRequestProto create_req = getCreateRequestProto("basic_file");
    hadoop::hdfs::CreateResponseProto create_resp;
    ASSERT_TRUE(client->create_file(create_req, create_resp));

    hadoop::hdfs::HdfsFileStatusProto file_status = create_resp.fs();
    ASSERT_EQ(file_status.filetype(), hadoop::hdfs::HdfsFileStatusProto::IS_FILE);
    ASSERT_TRUE(client->file_exists("basic_file"));
}

TEST_F(NamenodeTest, createExistingFile) {
    // First, create a file.
    hadoop::hdfs::CreateRequestProto create_req = getCreateRequestProto("test_file");
    hadoop::hdfs::CreateResponseProto create_resp;
    ASSERT_TRUE(client->create_file(create_req, create_resp));
    ASSERT_TRUE(client->file_exists("test_file"));

    // Then, try to create it again
    ASSERT_FALSE(client->create_file(create_req, create_resp));
}

TEST_F(NamenodeTest, createChildFile) {
    // Create a file in a folder that doesn't already exist
    hadoop::hdfs::CreateRequestProto create_req = getCreateRequestProto("/test_dir/child_file");
    create_req.set_createparent(true);
    hadoop::hdfs::CreateResponseProto create_resp;
    ASSERT_TRUE(client->create_file(create_req, create_resp));

    // Check to see that the folder was created
    hadoop::hdfs::GetListingRequestProto listing_req;
    hadoop::hdfs::GetListingResponseProto listing_resp;
    zkclient::ZkNnClient::ListingResponse client_response;
    listing_req.set_src("/test_dir");
    listing_req.set_startafter(0);
    listing_req.set_needlocation(false);
    client_response = client->get_listing(listing_req, listing_resp);
    ASSERT_EQ(client_response, zkclient::ZkNnClient::ListingResponse::Ok) << "File not found";

    // Check that we've gotten exactly the file we expected.
    hadoop::hdfs::DirectoryListingProto dir_listing;
    hadoop::hdfs::HdfsFileStatusProto file_status;
    dir_listing = listing_resp.dirlist();
    EXPECT_EQ(dir_listing.partiallisting_size(), 1);
    file_status = dir_listing.partiallisting(0);
    ASSERT_EQ(file_status.filetype(), hadoop::hdfs::HdfsFileStatusProto::IS_FILE);
    ASSERT_TRUE(file_status.path() == "/test_dir/child_file");
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
    ASSERT_EQ(client_response, zkclient::ZkNnClient::ListingResponse::Ok) << "File not found";

    // Create a file in a folder that already exists
    hadoop::hdfs::CreateRequestProto create_req = getCreateRequestProto("/existing_dir/child_file");
    create_req.set_createparent(true);
    hadoop::hdfs::CreateResponseProto create_resp;
    ASSERT_TRUE(client->create_file(create_req, create_resp));

    // Check that we've gotten exactly the file we expected.
    client_response = client->get_listing(listing_req, listing_resp);
    hadoop::hdfs::DirectoryListingProto dir_listing;
    hadoop::hdfs::HdfsFileStatusProto file_status;
    dir_listing = listing_resp.dirlist();
    EXPECT_EQ(dir_listing.partiallisting_size(), 1);
    file_status = dir_listing.partiallisting(0);
    ASSERT_EQ(file_status.filetype(), hadoop::hdfs::HdfsFileStatusProto::IS_FILE);
    ASSERT_TRUE(file_status.path() == "/existing_dir/child_file");
}