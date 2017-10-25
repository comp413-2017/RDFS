// Copyright 2017 Rice University, COMP 413 2017

#include <iostream>
#include "NameNodeTest.h"

TEST_F(NamenodeTest, getOneFileListing) {
    // First, create a file.
    hadoop::hdfs::CreateRequestProto create_req;
    hadoop::hdfs::CreateResponseProto create_resp;
    create_req.set_src("/testing/list_testing");
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
    listing_req.set_src("/testing/list_testing");
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
    ASSERT_EQ(file_status.path(), "/testing/list_testing");
}

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
}

TEST_F(NamenodeTest, failToListNonexistentFile) {
    // Attempt to get the listing
    hadoop::hdfs::GetListingRequestProto listing_req;
    hadoop::hdfs::GetListingResponseProto listing_resp;
    zkclient::ZkNnClient::ListingResponse client_response;
    listing_req.set_src("/testing/list_testing3");
    listing_req.set_startafter(0);
    listing_req.set_needlocation(false);
    client_response = client->get_listing(listing_req, listing_resp);
    ASSERT_EQ(client_response,
              zkclient::ZkNnClient::ListingResponse::FileDoesNotExist);
}

TEST_F(NamenodeTest, listingPerformance) {
    el::Loggers::setVerboseLevel(9);

    int i = 0;
    std::vector<int> depths = {5, 100};
    std::vector<int> dir_nums = {10, 100, 1000};
    std::vector<int> num_requests = {10, 100, 1000};

    for (auto depth : depths) {
        for (auto dir_num : dir_nums) {
            auto dir_string = "/testing/list_testing_depth" +
                std::to_string(depth) +
                "_size" + std::to_string(dir_num) + "/";

            for (i = 0; i < depth - 2; i++) {
                dir_string += std::to_string(i) + "/";
            }

            for (i = 0; i < dir_num; i++) {
                hadoop::hdfs::CreateRequestProto create_req;
                hadoop::hdfs::CreateResponseProto create_resp;
                create_req.set_src(dir_string + std::to_string(i));
                create_req.set_clientname("test_client_name");
                create_req.set_createparent(true);
                create_req.set_blocksize(0);
                create_req.set_replication(1);
                create_req.set_createflag(0);
                EXPECT_EQ(client->create_file(create_req, create_resp),
                          zkclient::ZkNnClient::CreateResponse::Ok);
            }
        }
    }

    hadoop::hdfs::GetListingRequestProto listing_req;
    hadoop::hdfs::GetListingResponseProto listing_resp;
    zkclient::ZkNnClient::ListingResponse client_response;

    for (auto depth : depths) {
        for (auto dir_num : dir_nums) {
            for (auto num_request : num_requests) {
                auto timer_string = "get_listing_depth" +
                        std::to_string(depth) +
                        "_size" + std::to_string(dir_num) +
                        "_requests" + std::to_string(num_request);
                TIMED_SCOPE_IF(listingPerformanceObj,
                               timer_string,
                               VLOG_IS_ON(9));

                auto dir_string = "/testing/list_testing_depth" +
                    std::to_string(depth) +
                    "_size" + std::to_string(dir_num) + "/";

                for (i = 0; i < depth - 2; i++) {
                    dir_string += std::to_string(i) + "/";
                }

                for (i = 0; i < num_request; i++) {
                    listing_req.set_src(dir_string);
                    listing_req.set_startafter(0);
                    listing_req.set_needlocation(false);
                    client_response = client->get_listing(listing_req,
                                                          listing_resp);
                    EXPECT_EQ(client_response,
                              zkclient::ZkNnClient::ListingResponse::Ok)
                              << "File not found";

                    // Check that we've gotten the number of files we expected.
                    hadoop::hdfs::DirectoryListingProto dir_listing;
                    hadoop::hdfs::HdfsFileStatusProto file_status;
                    EXPECT_TRUE(listing_resp.has_dirlist());
                    dir_listing = listing_resp.dirlist();
                    EXPECT_EQ(dir_listing.partiallisting_size(), dir_num);
                }
            }
        }
    }
}
