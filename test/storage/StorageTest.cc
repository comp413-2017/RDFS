// Copyright 2017 Rice University, COMP 413 2017

#include <easylogging++.h>
#include <gtest/gtest.h>
#include <zk_nn_client.h>
#include <util.h>
#include <zkwrapper.h>
#include "ClientNamenodeProtocolImpl.h"

#include "StorageMetrics.h"

#define ELPP_THREAD_SAFE

INITIALIZE_EASYLOGGINGPP

static const int NUM_DATANODES = 3;

using asio::ip::tcp;
using client_namenode_translator::ClientNamenodeTranslator;

namespace {

class StorageTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    int error_code;
    zk = std::make_shared<ZKWrapper>(
        "localhost:2181,localhost:2182,localhost:2183", error_code, "/testing");
    assert(error_code == 0);  // Z_OK

    // Start the namenode in a way that gives us a local pointer to zkWrapper.
    unsigned short port = 5351;
    nncli = new zkclient::ZkNnClient(zk);
    nncli->register_watches();
    nn_translator = new ClientNamenodeTranslator(port, nncli);
  }

  // Objects declared here can be used by all tests below.
  zkclient::ZkNnClient *nncli;
  ClientNamenodeTranslator *nn_translator;
  std::shared_ptr<ZKWrapper> zk;
};

/**
 * The following is an example of using StorageMetrics.
 * This test puts a file into RDFS, then prints 2 metrics (SD and % space used)
 */
TEST_F(StorageTest, testExample) {
  asio::io_service io_service;

  RPCServer namenodeServer = nn_translator->getRPCServer();
  std::thread(&RPCServer::serve, namenodeServer, std::ref(io_service))
      .detach();

  ASSERT_EQ(0, system("python "
                          "/home/vagrant/rdfs/test/integration/generate_file.py"
                          " > expected_""testfile1234"));
  // Put a file into rdfs.
  system(
      "hdfs dfs -fs hdfs://localhost:5351 -D dfs.blocksize=1048576 "
          "-copyFromLocal expected_testfile1234 /f");
  sleep(5);

  StorageMetrics metrics(NUM_DATANODES, zk);
  LOG(INFO) << " ---- Standard Deviation of blocks per DataNode: " <<
                                                 metrics.blocksPerDataNodeSD();
  LOG(INFO) << " ---- Fraction of total space used: " <<
                                                  metrics.usedSpaceFraction();
}

/**
 * This test checks that the hierarchical naming scheme generates appropriate
 * block_ids, block_group_ids, and storage_block_ids, and that the hierarchical
 * naming scheme allows us to convert between (storage_block_id, index) and
 * block_group_id correctly.
 * 
 * TODO(Adam): add testing for zk functionality and file associations once
 * those are implemented in the codebase.
 */
TEST_F(StorageTest, testIDGeneration) {
  // Use add_block_group to generate block_group_ids and storage_block_ids
  // Generate bogus filename & datanode strings:
  // add_block_group takes them as parameters but does not yet use them
  const std::string filename = "bogus";
  std::vector<std::string> dataNodes = {"bogus"};

  // TODO(Adam): Change to nncli.get_total_num_storage_blocks once it's
  // implemented
  uint32_t num_storage_blocks = 9;

  // TODO(Adam): use a real ecID
  std::pair<uint32_t, uint32_t> data_parity_blocks =
    get_num_data_parity_blocks((uint32_t)0);
  ASSERT_EQ(num_storage_blocks, data_parity_blocks.first +
    data_parity_blocks.second);

  uint64_t block_group_id = malloc(sizeof(uint64_t));
  uint64_t block_id = malloc(sizeof(uint64_t));
  uint64_t storage_blocks[num_storage_blocks];
  ZkNnClient::add_block_group(filename, block_group_id, datanodes,
                        num_storage_blocks);
  util.generate_block_id(block_id);

  // Make sure block ids are valid: 1st bit signifies EC/block_group(1)
  // or Replication/contiguous(0)
  // and last 16 bits of EC are 0 (for hierarchical naming scheme)
  ASSERT_EQ(1, ZkNnClient::is_ec_block(block_group_id));
  ASSERT_EQ(0, ZkNnClient::is_ec_block(block_id));

  uint64_t bit1_mask = (1ull << 63);  // one 1 and 63 0's
  uint64_t bit16_mask = ~(~(0ull) << 16)  // 48 0's and 16 1's
  ASSERT_EQ(0ull, (block_id) & bit1_mask);
  ASSERT_EQ(1ull, (block_group_id & bit1_mask) >> 63);
  ASSERT_EQ(0ull, block_group_id & bit16_mask);


  // Make sure we can generate block group id and index from storage block id
  int i;
  for (i = 0; i < num_storage_blocks; i++) {
    ASSERT_EQ(block_group_id, ZkNnClient::get_block_group_id(storage_blocks[i]));
    ASSERT_EQ(i, ZkNnClient::get_index_within_block_group(storage_blocks[i]));
  }
}

}  // namespace

static inline void print_usage() {
  LOG(ERROR) << "Storage test takes 0 or 1 arguments. \n"
      "-h prints this message. \n"
      "no argument runs the tests.";
}

static inline int runTests(int argc, char **argv) {
  // Start up zookeeper
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh start");
  sleep(10);
  system("/home/vagrant/zookeeper/bin/zkCli.sh rmr /testing");
  sleep(5);
  system("rm -f expected_testfile1234 actual_testfile* temp* tfs*");

  // initialize NUM_DATANODES datanodes
  int32_t xferPort = 50010;
  int32_t ipcPort = 50020;
  for (int i = 0; i < NUM_DATANODES; i++) {
    system(("truncate tfs" + std::to_string(i) + " -s 1000000000").c_str());
    std::string dnCliArgs = "-x " +
        std::to_string(xferPort + i) + " -p " + std::to_string(ipcPort + i)
        + " -b tfs" + std::to_string(i) + " &";
    std::string cmdLine =
        "bash -c \"exec -a StorageTestServer" + std::to_string(i) +
            " /home/vagrant/rdfs/build/rice-datanode/datanode " +
            dnCliArgs + "\" & ";
    system(cmdLine.c_str());
    sleep(3);
  }

  // Initialize and run the tests
  ::testing::InitGoogleTest(&argc, argv);
  int res = RUN_ALL_TESTS();

  // Kill the DataNodes and NameNodes, and stop zookeeper
  system("pkill -f StorageTestServer*");
  system("pkill -f namenode");
  system("/home/vagrant/zookeeper/bin/zkCli.sh rmr /testing");
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
  return res;
}

int main(int argc, char **argv) {
  if (argc > 2) {
    print_usage();
  }
  if (argc == 2 && strcmp(argv[1], "-h")) {
    print_usage();
    return 0;
  }
  return runTests(argc, argv);
}
