// Copyright 2017 Rice University, COMP 413 2017

#include <iostream>
#include "NameNodeTest.h"

TEST_F(NamenodeTest, cacheOneFile) {
    // First, create a file.
    hadoop::hdfs::CreateRequestProto create_req;
    hadoop::hdfs::CreateResponseProto create_resp;
    create_req.set_src("/testing/cache_testing");
    create_req.set_clientname("test_client_name");
    create_req.set_createparent(true);
    create_req.set_blocksize(0);
    create_req.set_replication(1);
    create_req.set_createflag(0);
    ASSERT_EQ(client->create_file(create_req, create_resp),
              zkclient::ZkNnClient::CreateResponse::Ok);

    // Now attempt to get the listing
    hadoop::hdfs::GetListingRequestProto listing_req;
    hadoop::hdfs::GetListingResponseProto listing_resp;
    zkclient::ZkNnClient::ListingResponse client_response;
    listing_req.set_src("/testing/cache_testing");
    listing_req.set_startafter(0);
    listing_req.set_needlocation(false);
    client_response = client->get_listing(listing_req, listing_resp);
    ASSERT_EQ(client_response,
              zkclient::ZkNnClient::ListingResponse::Ok) << "File not found";

    // Check that we've gotten exactly the file we expected.
    hadoop::hdfs::DirectoryListingProto dir_listing;
    hadoop::hdfs::HdfsFileStatusProto file_status;
    ASSERT_TRUE(listing_resp.has_dirlist());
    dir_listing = listing_resp.dirlist();
    EXPECT_EQ(dir_listing.partiallisting_size(), 1);
    file_status = dir_listing.partiallisting(0);
    ASSERT_EQ(file_status.filetype(),
              hadoop::hdfs::HdfsFileStatusProto::IS_FILE);
    ASSERT_EQ(file_status.path(), "/testing/cache_testing");

    // Check to make sure the cache has been accessed
    ASSERT_EQ(client->cache_size(), 1);
    ASSERT_TRUE(client->cache_contains("/testing/cache_testing"));

    // Now try to get the same file again
    client_response = client->get_listing(listing_req, listing_resp);
    ASSERT_EQ(client_response,
        zkclient::ZkNnClient::ListingResponse::Ok) << "File not found";

    // Check the listing information again
    ASSERT_TRUE(listing_resp.has_dirlist());
    dir_listing = listing_resp.dirlist();
    EXPECT_EQ(dir_listing.partiallisting_size(), 1);
    file_status = dir_listing.partiallisting(0);
    ASSERT_EQ(file_status.filetype(),
            hadoop::hdfs::HdfsFileStatusProto::IS_FILE);
    ASSERT_EQ(file_status.path(), "/testing/cache_testing");

    // Check to make sure cache isn't funky
    ASSERT_EQ(client->cache_size(), 1);
    ASSERT_TRUE(client->cache_contains("/testing/cache_testing"));
}

// try this on file and then access that file/change it, make sure cache is empty
TEST_F(NamenodeTest, modifyCache) {
    std::string src = "/testing/cache_testing";
    // First, create a file.
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

    // Now attempt to get the listing
    hadoop::hdfs::GetListingRequestProto listing_req;
    hadoop::hdfs::GetListingResponseProto listing_resp;
    zkclient::ZkNnClient::ListingResponse client_response;
    listing_req.set_src(src);
    listing_req.set_startafter(0);
    listing_req.set_needlocation(false);
    client_response = client->get_listing(listing_req, listing_resp);
    ASSERT_EQ(client_response,
            zkclient::ZkNnClient::ListingResponse::Ok) << "File not found";

    // Check to make sure the cache has been accessed
    ASSERT_EQ(client->cache_size(), 1);
    ASSERT_TRUE(client->cache_contains(src));

    // Complete the file
    hadoop::hdfs::CompleteRequestProto complete_req;
    hadoop::hdfs::CompleteResponseProto complete_resp;
    complete_req.set_src(src);
    client->complete(complete_req, complete_resp);

    // Now delete that file
    hadoop::hdfs::DeleteRequestProto delete_req;
    hadoop::hdfs::DeleteResponseProto delete_resp;
    delete_req.set_src(src);
    delete_req.set_recursive(false);
    ASSERT_EQ(client->destroy(delete_req, delete_resp),
    zkclient::ZkNnClient::DeleteResponse::Ok);
    client_response = client->get_listing(listing_req, listing_resp);
    ASSERT_EQ(client_response,
            zkclient::ZkNnClient::ListingResponse::Ok) << "File not found";

    // Check that the cache is empty now
    ASSERT_EQ(client->cache_size(), 0);
    ASSERT_FALSE(client->cache_contains(src));
}

// try this on a child of a directory, make sure directory is changed
TEST_F(NamenodeTest, getMultipleFilesFromDir) {
    // First, create some files in a directory.

    hadoop::hdfs::CreateRequestProto create_req;
    hadoop::hdfs::CreateResponseProto create_resp;
    create_req.set_src("/testing/list_testing2/file1");
    create_req.set_clientname("test_client_name");
    create_req.set_createparent(true);
    create_req.set_blocksize(0);
    create_req.set_replication(1);
    create_req.set_createflag(0);
    ASSERT_EQ(client->create_file(create_req, create_resp),
              zkclient::ZkNnClient::CreateResponse::Ok);

    create_req.set_src("/testing/list_testing2/file2");
    create_req.set_clientname("test_client_name");
    create_req.set_createparent(false);
    create_req.set_blocksize(0);
    create_req.set_replication(1);
    create_req.set_createflag(0);
    ASSERT_EQ(client->create_file(create_req, create_resp),
              zkclient::ZkNnClient::CreateResponse::Ok);

    create_req.set_src("/testing/list_testing2/file3");
    create_req.set_clientname("test_client_name");
    create_req.set_createparent(false);
    create_req.set_blocksize(0);
    create_req.set_replication(1);
    create_req.set_createflag(0);
    ASSERT_EQ(client->create_file(create_req, create_resp),
              zkclient::ZkNnClient::CreateResponse::Ok);

    // Now attempt to list the directory
    hadoop::hdfs::GetListingRequestProto listing_req;
    hadoop::hdfs::GetListingResponseProto listing_resp;
    zkclient::ZkNnClient::ListingResponse client_response;
    listing_req.set_src("/testing/list_testing2");
    listing_req.set_startafter(0);
    listing_req.set_needlocation(false);
    client_response = client->get_listing(listing_req, listing_resp);
    ASSERT_EQ(client_response,
              zkclient::ZkNnClient::ListingResponse::Ok) << "File not found";

    // Check that we've gotten exactly the files we expected.
    hadoop::hdfs::DirectoryListingProto dir_listing;
    hadoop::hdfs::HdfsFileStatusProto file_status;
    ASSERT_TRUE(listing_resp.has_dirlist());
    dir_listing = listing_resp.dirlist();
    EXPECT_EQ(dir_listing.partiallisting_size(), 3);
    file_status = dir_listing.partiallisting(0);
    ASSERT_EQ(file_status.filetype(),
              hadoop::hdfs::HdfsFileStatusProto::IS_FILE);
    ASSERT_TRUE(file_status.path() == "/testing/list_testing2/file1" ||
                        file_status.path() == "/testing/list_testing2/file2" ||
                        file_status.path() == "/testing/list_testing2/file3");
    file_status = dir_listing.partiallisting(1);
    ASSERT_EQ(file_status.filetype(),
              hadoop::hdfs::HdfsFileStatusProto::IS_FILE);
    ASSERT_TRUE(file_status.path() == "/testing/list_testing2/file1" ||
                file_status.path() == "/testing/list_testing2/file2" ||
                file_status.path() == "/testing/list_testing2/file3");
    file_status = dir_listing.partiallisting(2);
    ASSERT_EQ(file_status.filetype(),
              hadoop::hdfs::HdfsFileStatusProto::IS_FILE);
    ASSERT_TRUE(file_status.path() == "/testing/list_testing2/file1" ||
                file_status.path() == "/testing/list_testing2/file2" ||
                file_status.path() == "/testing/list_testing2/file3");

    // Check to make sure the cache has been accessed
    ASSERT_EQ(client->cache_size(), 1);
    ASSERT_TRUE(client->cache_contains("/testing/list_testing2"));

    // Now delete a child and see if the parent is still in the cache (it shouldn't)
    hadoop::hdfs::DeleteRequestProto delete_req;
    hadoop::hdfs::DeleteResponseProto delete_resp;
    delete_req.set_src("/testing/list_testing2/file1");
    delete_req.set_recursive(false);
    ASSERT_EQ(client->destroy(delete_req, delete_resp),
        zkclient::ZkNnClient::DeleteResponse::Ok);

    // Check to make sure the cache has been accessed
    ASSERT_EQ(client->cache_size(), 0);
    ASSERT_FALSE(client->cache_contains("/testing/list_testing2"));
}
