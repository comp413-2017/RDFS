// Copyright 2017 Rice University, COMP 413 2017

#include "NameNodeTest.h"

// Ensure that a lease is created successfully and that it does its job,
// by making sure another client can't request a lease on the same file

TEST_F(NamenodeTest, checkLeaseTest) {
  LOG(INFO) << "In checkLeaseCorrectnessTest";

  std::string client_name = "test_client";

  hadoop::hdfs::CreateRequestProto create_req =
      getCreateRequestProto("test_file");
  hadoop::hdfs::CreateResponseProto create_resp;

  ASSERT_EQ(client->create_file(create_req, create_resp),
            zkclient::ZkNnClient::CreateResponse::Ok);

  ASSERT_TRUE(client->check_lease(client_name, "test_file"));
}

// TEST_F(NamenodeTest, checkLeaseTest) {
//  LOG(INFO) << "In checkLeaseCorrectnessTest";
//
//  // Create a file in a folder that doesn't already exist
//  hadoop::hdfs::RenewLeaseRequestProto renew_lease_req;
//  hadoop::hdfs::RenewLeaseResponseProto renew_lease_res;
//  renew_lease_req.set_clientname("test_client");
//  client->renew_lease(renew_lease_req, renew_lease_res);
//  uint64_t time = client->current_time_ms();
//  bool exists;
//  int error_code;
//  ASSERT_TRUE(client->zk->exists(client->CLIENTS
//                                 + std::string("/test_client"),
//                                 exists, error_code));
//  ASSERT_TRUE(exists);
//  uint64_t ONE_MIN = 1 * 60 * 1000;
//  ASSERT_TRUE(time - client->
//      get_client_lease_timestamp("test_client") < ONE_MIN);
// }
