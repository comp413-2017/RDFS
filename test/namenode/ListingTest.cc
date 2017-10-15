// Copyright 2017 Rice University, COMP 413 2017

#include "NameNodeTest.h"

TEST_F(NamenodeTest, getOneFileListing) {
    // First, create a file.
    hadoop::hdfs::CreateRequestProto create_req;
    hadoop::hdfs::CreateResponseProto create_resp;
    create_req.set_src("/list_testing");
    create_req.set_clientname("test_client_name");
    create_req.set_createparent(false);
    create_req.set_blocksize(0);
    create_req.set_replication(1);
    create_req.set_createflag(0);
    ASSERT_TRUE(client->create_file(create_req, create_resp));

    // Now attempt to get the listing
    hadoop::hdfs::GetListingRequestProto listing_req;
    hadoop::hdfs::GetListingResponseProto listing_resp;
    zkclient::ZkNnClient::ListingResponse client_response;
    listing_req.set_src("/list_testing");
    listing_req.set_startafter(0);
    listing_req.set_needlocation(false);
    client_response = client->get_listing(listing_req, listing_resp);
    ASSERT_EQ(client_response, zkclient::ZkNnClient::ListingResponse::Ok) << "File not found";

    // Check that we've gotten exactly the file we expected.
    hadoop::hdfs::DirectoryListingProto dir_listing;
    hadoop::hdfs::HdfsFileStatusProto file_status;
    ASSERT_TRUE(listing_resp.has_dirlist());
    dir_listing = listing_resp.dirlist();
    EXPECT_EQ(dir_listing.partiallisting_size(), 1);
    file_status = dir_listing.partiallisting(0);
    ASSERT_EQ(file_status.filetype(), hadoop::hdfs::HdfsFileStatusProto::IS_FILE);
    ASSERT_EQ(file_status.path(), "/list_testing");
}

TEST_F(NamenodeTest, getMultipleFilesFromDir) {
    // First, create some files in a directory.
    hadoop::hdfs::CreateRequestProto create_req;
    hadoop::hdfs::CreateResponseProto create_resp;
    create_req.set_src("/list_testing2/file1");
    create_req.set_clientname("test_client_name");
    create_req.set_createparent(true);
    create_req.set_blocksize(0);
    create_req.set_replication(1);
    create_req.set_createflag(0);
    ASSERT_TRUE(client->create_file(create_req, create_resp));

    create_req.set_src("/list_testing2/file2");
    create_req.set_clientname("test_client_name");
    create_req.set_createparent(false);
    create_req.set_blocksize(0);
    create_req.set_replication(1);
    create_req.set_createflag(0);
    ASSERT_TRUE(client->create_file(create_req, create_resp));

    create_req.set_src("/list_testing2/file3");
    create_req.set_clientname("test_client_name");
    create_req.set_createparent(false);
    create_req.set_blocksize(0);
    create_req.set_replication(1);
    create_req.set_createflag(0);
    ASSERT_TRUE(client->create_file(create_req, create_resp));

    // Now attempt to list the directory
    hadoop::hdfs::GetListingRequestProto listing_req;
    hadoop::hdfs::GetListingResponseProto listing_resp;
    zkclient::ZkNnClient::ListingResponse client_response;
    listing_req.set_src("/list_testing2");
    listing_req.set_startafter(0);
    listing_req.set_needlocation(false);
    client_response = client->get_listing(listing_req, listing_resp);
    ASSERT_EQ(client_response, zkclient::ZkNnClient::ListingResponse::Ok) << "File not found";

    // Check that we've gotten exactly the files we expected.
    hadoop::hdfs::DirectoryListingProto dir_listing;
    hadoop::hdfs::HdfsFileStatusProto file_status;
    ASSERT_TRUE(listing_resp.has_dirlist());
    dir_listing = listing_resp.dirlist();
    EXPECT_EQ(dir_listing.partiallisting_size(), 3);
    file_status = dir_listing.partiallisting(0);
    ASSERT_EQ(file_status.filetype(), hadoop::hdfs::HdfsFileStatusProto::IS_FILE);
    ASSERT_TRUE(file_status.path() == "/list_testing2/file1" ||
                        file_status.path() == "/list_testing2/file2" ||
                        file_status.path() == "/list_testing2/file3");
    file_status = dir_listing.partiallisting(1);
    ASSERT_EQ(file_status.filetype(), hadoop::hdfs::HdfsFileStatusProto::IS_FILE);
    ASSERT_TRUE(file_status.path() == "/list_testing2/file1" ||
                file_status.path() == "/list_testing2/file2" ||
                file_status.path() == "/list_testing2/file3");
    file_status = dir_listing.partiallisting(2);
    ASSERT_EQ(file_status.filetype(), hadoop::hdfs::HdfsFileStatusProto::IS_FILE);
    ASSERT_TRUE(file_status.path() == "/list_testing2/file1" ||
                file_status.path() == "/list_testing2/file2" ||
                file_status.path() == "/list_testing2/file3");
}

TEST_F(NamenodeTest, failToListNonexistentFile) {
    // Attempt to get the listing
    hadoop::hdfs::GetListingRequestProto listing_req;
    hadoop::hdfs::GetListingResponseProto listing_resp;
    zkclient::ZkNnClient::ListingResponse client_response;
    listing_req.set_src("/list_testing3");
    listing_req.set_startafter(0);
    listing_req.set_needlocation(false);
    client_response = client->get_listing(listing_req, listing_resp);
    ASSERT_EQ(client_response, zkclient::ZkNnClient::ListingResponse::FileDoesNotExist);
}
