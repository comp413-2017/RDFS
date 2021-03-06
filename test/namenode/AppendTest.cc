// Copyright 2017 Rice University, COMP 413 2017

#include "NameNodeTest.h"
#include "zk_nn_client.h"

// Ensure that a lease is created successfully and that it does its job,
// by making sure another client can't request a lease on the same file

TEST_F(NamenodeTest, checkLeaseTest) {
  LOG(INFO) << "In checkLeaseCorrectnessTest";

  std::string client_name = "test_client_mjp";
  std::string file_path = "test_file_mjp";

  // Add file to system!
  hadoop::hdfs::CreateRequestProto create_req =
      getCreateRequestProto(file_path);
  hadoop::hdfs::CreateResponseProto create_resp;

  ASSERT_EQ(client->create_file(create_req, create_resp),
            zkclient::ZkNnClient::CreateResponse::Ok);

  // Now add client to system!
  hadoop::hdfs::RenewLeaseRequestProto renew_lease_req;
  hadoop::hdfs::RenewLeaseResponseProto renew_lease_res;
  renew_lease_req.set_clientname(client_name);
  client->renew_lease(renew_lease_req, renew_lease_res);

  // Call check lease, and then check the zk structure!
  ASSERT_TRUE(client->check_lease(client_name, file_path));
  bool exists;
  int error_code;

  // Check that client is added to /file_path/leases branch of zk tree.
  ASSERT_TRUE(client->zk->exists("/fileSystem/" + file_path +
              "/leases/" + client_name, exists, error_code));

  ASSERT_TRUE(exists);

  // Call check lease again with same client
  ASSERT_TRUE(client->check_lease(client_name, file_path));
  // Check that client is in /file_path/leases branch of zk tree.
  ASSERT_TRUE(client->zk->exists("/fileSystem/" + file_path +
              "/leases/" + client_name, exists, error_code));
  ASSERT_TRUE(exists);

  // Now try adding a dumb child! Then it should fail.
  std::string client2 = "test_client_2_mjp";
  hadoop::hdfs::RenewLeaseRequestProto renew_lease_req2;
  hadoop::hdfs::RenewLeaseResponseProto renew_lease_res2;
  renew_lease_req2.set_clientname(client2);
  client->renew_lease(renew_lease_req2, renew_lease_res2);

  ASSERT_TRUE(zk->create("/fileSystem/" + file_path +
                  "/leases/" + client2, ZKWrapper::EMPTY_VECTOR, error_code,
                         false));
  ASSERT_FALSE(client->check_lease(client_name, file_path));
}

TEST_F(NamenodeTest, processRequestTest) {
  std::string client_name = "test_client_mjp_3";
  std::string file_path = "test_file_mjp_3";

  hadoop::hdfs::AppendRequestProto append_req;
  append_req.set_clientname(client_name);
  append_req.set_src(file_path);

  // Verify that the file doesn't exist.
  ASSERT_FALSE(client->process_request(client_name, file_path, append_req));

  // Now add the file path in, but verify that the client still doesn't exist.
  hadoop::hdfs::CreateRequestProto create_req =
      getCreateRequestProto(file_path);
  hadoop::hdfs::CreateResponseProto create_resp;
  ASSERT_EQ(client->create_file(create_req, create_resp),
            zkclient::ZkNnClient::CreateResponse::Ok);
  // This client should be able to append
  ASSERT_TRUE(client->process_request(client_name, file_path, append_req));
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
