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

    hadoop::hdfs::GetListingRequestProto listing_req;
    hadoop::hdfs::GetListingResponseProto listing_resp;
    listing_req.set_src("/list_testing");
    listing_req.set_startAfter(0);
    listing_req.set_needLocation(false);
    client->get_listing(listing_req, listing_resp);
}
