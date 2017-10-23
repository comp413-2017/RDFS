// Copyright 2017 Rice University, COMP 413 2017

#include "NameNodeTest.h"

INITIALIZE_EASYLOGGINGPP

void NamenodeTest::SetUp() {
    int error_code;
    auto zk_shared =
            std::make_shared<ZKWrapper>("localhost:2181",
            error_code, "/testing");
    assert(error_code == 0);  // Z_OK
    client = new zkclient::ZkNnClient(zk_shared);
    zk = new ZKWrapper("localhost:2181", error_code, "/testing");
}

hadoop::hdfs::CreateRequestProto NamenodeTest::getCreateRequestProto(
        const std::string &path
) {
    hadoop::hdfs::CreateRequestProto create_req;
    create_req.set_src(path);
    create_req.set_clientname("asdf");
    create_req.set_createparent(false);
    create_req.set_blocksize(1);
    create_req.set_replication(1);
    create_req.set_createflag(0);
    return create_req;
}

TEST_F(NamenodeTest, checkNamespace) {
  // nuffin
}

TEST_F(NamenodeTest, findDataNodes) {
  int error;
  zk->create("/health/localhost:2181", ZKWrapper::EMPTY_VECTOR, error, false);
  zk->create("/health/localhost:2181/heartbeat",
             ZKWrapper::EMPTY_VECTOR,
             error, true);
  zk->create("/health/localhost:2182", ZKWrapper::EMPTY_VECTOR, error, false);
  zk->create("/health/localhost:2182/heartbeat",
             ZKWrapper::EMPTY_VECTOR,
             error, true);

  zkclient::DataNodePayload data_node_payload = zkclient::DataNodePayload();
  data_node_payload.ipcPort = 1;
  data_node_payload.xferPort = 1;
  data_node_payload.disk_bytes = 1;
  data_node_payload.free_bytes = 1024;
  data_node_payload.xmits = 5;

  std::vector<uint8_t> stats_vec;
  stats_vec.resize(sizeof(zkclient::DataNodePayload));
  memcpy(&stats_vec[0], &data_node_payload, sizeof(zkclient::DataNodePayload));
  ASSERT_TRUE(zk->create("/health/localhost:2181/stats",
                         stats_vec, error, true));

  data_node_payload.xmits = 3;
  memcpy(&stats_vec[0], &data_node_payload, sizeof(zkclient::DataNodePayload));
  ASSERT_TRUE(zk->create("/health/localhost:2182/stats",
                         stats_vec, error, true));

  auto datanodes = std::vector<std::string>();
  u_int64_t block_id;
  util::generate_uuid(block_id);

  zkclient::BlockZNode block_data;
  block_data.block_size = 64;
  std::vector<std::uint8_t> data_vect(sizeof(block_data));
  memcpy(&data_vect[0], &block_data, sizeof(block_data));
  ASSERT_TRUE(zk->create("/block_locations/" + std::to_string(block_id),
                         data_vect,
                         error, false));

  LOG(INFO) << "Finding dn's for block " << block_id;
  int rep_factor = 1;
  client->find_datanode_for_block(datanodes,
                                  block_id,
                                  rep_factor,
                                  true,
                                  block_data.block_size);

  for (auto datanode : datanodes) {
    LOG(INFO) << "Returned datanode " << datanode;
  }
  ASSERT_EQ(rep_factor, datanodes.size());
  // Check that the DN with fewer transmists was returned
  ASSERT_EQ("localhost:2182", datanodes[0]);
}

TEST_F(NamenodeTest, findDataNodesWithReplicas) {
  // Check if we can find datanodes, without overlapping with ones that
  // already contain a replica
}

TEST_F(NamenodeTest, basicCheckAcks) {
  // Check if check_acks works as intended
  int error;
  zk->delete_node("/work_queues/wait_for_acks/block_uuid_1/dn-id-3", error);
  zk->delete_node("/work_queues/wait_for_acks/block_uuid_1/dn-id-2", error);
  zk->delete_node("/work_queues/wait_for_acks/block_uuid_1/dn-id-1", error);
  zk->delete_node("/work_queues/wait_for_acks/block_uuid_1", error);

  auto data = std::vector<std::uint8_t>();
  data.push_back(3);
  zk->create("/work_queues/wait_for_acks/block_uuid_1", data, error, false);
  ASSERT_EQ(0, error);

  zk->create("/work_queues/wait_for_acks/block_uuid_1/dn-id-1",
             ZKWrapper::EMPTY_VECTOR,
             error, false);
  ASSERT_EQ(0, error);

  // Only one DN acknowledged, but not timed out, so should succeed
  ASSERT_EQ(true, client->check_acks());

  zk->create("/work_queues/wait_for_acks/block_uuid_1/dn-id-2",
             ZKWrapper::EMPTY_VECTOR,
             error, false);
  ASSERT_EQ(0, error);
  // Only two DNs acknowledged, but not timed out, so should succeed
  ASSERT_EQ(true, client->check_acks());

  zk->create("/work_queues/wait_for_acks/block_uuid_1/dn-id-3",
             ZKWrapper::EMPTY_VECTOR,
             error, false);
  ASSERT_EQ(0, error);
  // All three DNs acknowledged, so should succeed
  ASSERT_EQ(true, client->check_acks());


  // Since enough DNs acknowledged, the block_uuid_1 should have been removed
  // from wait_for_acks
  auto children = std::vector<std::string>();
  zk->get_children("/work_queues/wait_for_acks", children, error);
  ASSERT_EQ(0, children.size());
}

TEST_F(NamenodeTest, previousBlockComplete) {
  int error;
  u_int64_t block_id;
  block_id = 0;
  LOG(INFO) << "Previous block_id is " << block_id;
  // Calling previousblockcomplete on the first block should be true.
  ASSERT_EQ(true, client->previousBlockComplete(block_id));
  util::generate_uuid(block_id);
  /* mock the directory */
  zk->create("/block_locations", ZKWrapper::EMPTY_VECTOR, error, false);
  zk->create("/block_locations/" + std::to_string(block_id),
             ZKWrapper::EMPTY_VECTOR,
             error, false);
  ASSERT_EQ(false, client->previousBlockComplete(block_id));
  /* mock the child directory */
  zk->create("/block_locations/" + std::to_string(block_id) + "/child1",
             ZKWrapper::EMPTY_VECTOR,
             error, false);
  ASSERT_EQ(true, client->previousBlockComplete(block_id));
}

TEST_F(NamenodeTest, testRenameFile) {
  int error_code;

  // Create a test file for renaming
  hadoop::hdfs::CreateRequestProto create_req;
  hadoop::hdfs::CreateResponseProto create_resp;
  create_req.set_src("/old_name");
  create_req.set_clientname("test_client_name");
  create_req.set_createparent(false);
  create_req.set_blocksize(0);
  create_req.set_replication(1);
  create_req.set_createflag(0);
  ASSERT_EQ(client->create_file(create_req, create_resp),
    zkclient::ZkNnClient::CreateResponse::Ok);

  // Create a child of the old file with a fake block
  std::string new_path;
  zk->create_sequential("/fileSystem/old_name/block-",
                        zk->get_byte_vector("Block uuid"),
                        new_path,
                        false,
                        error_code);
  ASSERT_EQ(0, error_code);
  ASSERT_EQ("/fileSystem/old_name/block-0000000000", new_path);

  // Rename
  hadoop::hdfs::RenameRequestProto rename_req;
  hadoop::hdfs::RenameResponseProto rename_resp;
  rename_req.set_src("/old_name");
  rename_req.set_dst("/new_name");
  client->rename(rename_req, rename_resp);
  ASSERT_TRUE(rename_resp.result());

  // Ensure that the renamed node has the same data
  zkclient::FileZNode renamed_data;
  std::vector<std::uint8_t> data(sizeof(renamed_data));
  ASSERT_TRUE(zk->get("/fileSystem/new_name", data, error_code));
  std::uint8_t *buffer = &data[0];
  memcpy(&renamed_data, buffer, sizeof(renamed_data));
  ASSERT_EQ(1, renamed_data.replication);
  ASSERT_EQ(0, renamed_data.blocksize);
  ASSERT_EQ(2, renamed_data.filetype);

  // Ensure that the file's child indicating block_id was renamed as well
  auto new_block_data = std::vector<std::uint8_t>();
  zk->get("/fileSystem/new_name/block-0000000000", new_block_data, error_code);
  ASSERT_EQ(0, error_code);
  ASSERT_EQ("Block uuid",
            std::string(new_block_data.begin(), new_block_data.end()));

  // Ensure that old_name was delete
  bool exist;
  zk->exists("/fileSystem/old_name", exist, error_code);
  ASSERT_EQ(false, exist);
}

TEST_F(NamenodeTest, testRenameDirWithFiles) {
  int error_code;

  // Create a test file for renaming
  hadoop::hdfs::CreateRequestProto create_req;
  hadoop::hdfs::CreateResponseProto create_resp;
  create_req.set_src("/old_dir/file1");
  create_req.set_clientname("test_client_name");
  create_req.set_createparent(true);
  create_req.set_blocksize(0);
  create_req.set_replication(1);
  create_req.set_createflag(0);
  ASSERT_EQ(client->create_file(create_req, create_resp),
            zkclient::ZkNnClient::CreateResponse::Ok);
  create_req.set_src("/old_dir/file2");
  ASSERT_EQ(client->create_file(create_req, create_resp),
            zkclient::ZkNnClient::CreateResponse::Ok);
  create_req.set_src("/old_dir/nested_dir/nested_file");
  ASSERT_EQ(client->create_file(create_req, create_resp),
            zkclient::ZkNnClient::CreateResponse::Ok);

  // Rename
  hadoop::hdfs::RenameRequestProto rename_req;
  hadoop::hdfs::RenameResponseProto rename_resp;
  rename_req.set_src("/old_dir");
  rename_req.set_dst("/new_dir");
  client->rename(rename_req, rename_resp);
  ASSERT_TRUE(rename_resp.result());

  // // Ensure that the renamed node has the same data
  zkclient::FileZNode renamed_data;
  std::vector<std::uint8_t> data(sizeof(renamed_data));
  ASSERT_TRUE(zk->get("/fileSystem/new_dir", data, error_code));
  memcpy(&renamed_data, &data[0], sizeof(renamed_data));
  ASSERT_EQ(0, renamed_data.replication);
  ASSERT_EQ(0, renamed_data.blocksize);
  ASSERT_EQ(1, renamed_data.filetype);

  ASSERT_TRUE(zk->get("/fileSystem/new_dir/file1", data, error_code));
  memcpy(&renamed_data, &data[0], sizeof(renamed_data));
  ASSERT_EQ(1, renamed_data.replication);
  ASSERT_EQ(0, renamed_data.blocksize);
  ASSERT_EQ(2, renamed_data.filetype);

  ASSERT_TRUE(zk->get("/fileSystem/new_dir/file2", data, error_code));
  memcpy(&renamed_data, &data[0], sizeof(renamed_data));
  ASSERT_EQ(1, renamed_data.replication);
  ASSERT_EQ(0, renamed_data.blocksize);
  ASSERT_EQ(2, renamed_data.filetype);

  ASSERT_TRUE(zk->get("/fileSystem/new_dir/nested_dir/nested_file",
                      data,
                      error_code));
  memcpy(&renamed_data, &data[0], sizeof(renamed_data));
  ASSERT_EQ(1, renamed_data.replication);
  ASSERT_EQ(0, renamed_data.blocksize);
  ASSERT_EQ(2, renamed_data.filetype);

  // Ensure that file nodes were deleted
  bool exist;
  zk->exists("/fileSystem/old_dir", exist, error_code);
  ASSERT_EQ(false, exist);
}

int main(int argc, char **argv) {
    el::Configurations conf(LOG_CONFIG_FILE);
    conf.set(el::Level::Info, el::ConfigurationType::Enabled, "false");
    el::Loggers::reconfigureAllLoggers(conf);
    el::Loggers::addFlag(el::LoggingFlag::ColoredTerminalOutput);

    // Start up zookeeper
    system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
    system("sudo /home/vagrant/zookeeper/bin/zkServer.sh start");

    // In case testing files were not previously deleted.
    system("sudo /home/vagrant/zookeeper/bin/zkCli.sh rmr /testing");
    sleep(10);

    // Initialize and run the tests
    ::testing::InitGoogleTest(&argc, argv);
    int res = RUN_ALL_TESTS();
    // NOTE: You'll need to scroll up a bit to see the test results

    // Remove test files and shutdown zookeeper
    system("sudo /home/vagrant/zookeeper/bin/zkCli.sh rmr /testing");
    system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
    return res;
}
