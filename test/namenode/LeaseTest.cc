// Copyright 2017 Rice University, COMP 413 2017

#include <ClientNamenodeProtocol.pb.h>
#include "NameNodeTest.h"

// Ensure that a lease is created successfully and that it does its job,
// by making sure another client can't request a lease on the same file

TEST_F(NamenodeTest, renewLeaseCorrectnessTest) {
  LOG(INFO) << "In renewLeaseCorrectnessTest";
  // Create a file in a folder that doesn't already exist
  hadoop::hdfs::RenewLeaseRequestProto renew_lease_req;
  hadoop::hdfs::RenewLeaseResponseProto renew_lease_res;
  renew_lease_req.set_clientname("test_client");
  client->renew_lease(renew_lease_req, renew_lease_res);
  uint64_t time = client->current_time_ms();
  bool exists;
  int error_code;
  ASSERT_TRUE(client->zk->exists(client->CLIENTS
                                 + std::string("/test_client"),
                                 exists, error_code));
  ASSERT_TRUE(exists);
  uint64_t ONE_MIN = 1 * 60 * 1000;
  ASSERT_TRUE(time - client->get_client_lease_timestamp("test_client") < ONE_MIN);
}

TEST_F(NamenodeTest, recoverLeaseCorrectnessTest) {
  LOG(INFO) << "In recoverLeaseCorrectnessTest";

  // RecoverLease is expected to return true for a file that does
  // not exist in the system.
  hadoop::hdfs::RecoverLeaseRequestProto recover_lease_req;
  hadoop::hdfs::RecoverLeaseResponseProto recover_lease_res;

  recover_lease_req.set_clientname("test_client");
  recover_lease_req.set_src("test_filename");
  client->recover_lease(recover_lease_req, recover_lease_res);
  ASSERT_FALSE(recover_lease_res.result());

  // Create the file
  hadoop::hdfs::CreateRequestProto create_req =
    getCreateRequestProto("test_filename");
  hadoop::hdfs::CreateResponseProto create_resp;
  ASSERT_EQ(client->create_file(create_req, create_resp),
            zkclient::ZkNnClient::CreateResponse::Ok);

  // RecoverLease should return true given a file that does not have
  // a lease holder.
  client->recover_lease(recover_lease_req, recover_lease_res);
  ASSERT_TRUE(recover_lease_res.result());
}

// TODO: After append is implemented (where lease is actually acquired,
// more tests that check expiration are needed.)

TEST_F(NamenodeTest, issueLeaseTest) {
  // Open a lease on some file, through an append request
  // Check append status

  // Create another append request on the same file
  // with different client ID.
  // Ensure this returns the expected error code -- LeaseAlreadyExists

  ASSERT_TRUE(true);
}

TEST_F(NamenodeTest, renewLeaseTest) {
  // Open a lease on some file, through an append request
  // Check append status

  // Call renew lease, and then check the timestamp on
  // the lease associated with the file.

  ASSERT_TRUE(true);
}

// Performance could be measured by tracking the round-trip time of lease open
// and lease close
// Performance could be measured by tracking the time it takes to issue a lease
