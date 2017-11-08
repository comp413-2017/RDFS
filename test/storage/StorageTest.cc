// Copyright 2017 Rice University, COMP 413 2017

#include <easylogging++.h>
#include <gtest/gtest.h>
#include <zk_nn_client.h>
#include "ClientNamenodeProtocolImpl.h"

#include "StorageMetrics.h"
#include "../util/RDFSTestUtils.h"

#define ELPP_THREAD_SAFE

INITIALIZE_EASYLOGGINGPP

using asio::ip::tcp;
using client_namenode_translator::ClientNamenodeTranslator;
using RDFSTestUtils::initializeDatanodes;

static const int NUM_DATANODES = 3;

int32_t xferPort = 50010;
int32_t ipcPort = 50020;
int maxDatanodeId = 0;
// Use minDatanodId++ when you want to kill a datanode.
int minDatanodeId = 0;
// This is incremented for each test.
uint16_t nextPort = 5351;

static inline void initializeDatanodes(int numDatanodes) {
  initializeDatanodes(
      maxDatanodeId,
      numDatanodes,
      "StorageTestServer",
      xferPort,
      ipcPort);
  maxDatanodeId += numDatanodes;
  xferPort += numDatanodes;
  ipcPort += numDatanodes;
}

namespace {

class StorageTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    initializeDatanodes(NUM_DATANODES);

    int error_code;
    zk = std::make_shared<ZKWrapper>(
        "localhost:2181,localhost:2182,localhost:2183", error_code, "/testing");
    assert(error_code == 0);  // Z_OK

    // Start the namenode in a way that gives us a local pointer to zkWrapper.
    port = nextPort++;
    nncli = new zkclient::ZkNnClient(zk);
    nncli->register_watches();
    nn_translator = new ClientNamenodeTranslator(port, nncli);
  }

  virtual void TearDown() {
    system("pkill -f StorageTestServer*");
  }

  // Objects declared here can be used by all tests below.
  zkclient::ZkNnClient *nncli;
  ClientNamenodeTranslator *nn_translator;
  std::shared_ptr<ZKWrapper> zk;
  unsigned short port;
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
  sleep(3);

  ASSERT_EQ(0, system("python "
                          "/home/vagrant/rdfs/test/integration/generate_file.py"
                          " > expected_""testfile1234"));
  // Put a file into rdfs.
  system((
      "hdfs dfs -fs hdfs://localhost:" + std::to_string(port) +
          " -D dfs.blocksize=1048576 "
          "-copyFromLocal expected_testfile1234 /f").c_str());
  sleep(5);

  StorageMetrics metrics(zk);
  LOG(INFO) << " ---- Standard Deviation of blocks per DataNode: " <<
                                                 metrics.blocksPerDataNodeSD();

  LOG(INFO) << " ---- Fraction of total space used: " <<
                                                  metrics.usedSpaceFraction();
  system(("hdfs dfs -fs hdfs://localhost:" + std::to_string(port) + " -rm /f")
             .c_str());
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

  /*// TODO(Adam): use a real ecID
  std::pair<uint32_t, uint32_t> data_parity_blocks =
    nncli->get_num_data_parity_blocks((uint32_t)0);
  ASSERT_EQ(num_storage_blocks, data_parity_blocks.first +
    data_parity_blocks.second);*/

  uint64_t block_group_id;
  uint64_t block_id;
  std::vector<uint64_t> storage_blocks = {};
  std::vector<char> blockIndices;
//  nncli->add_block_group(filename, block_group_id, dataNodes,
//    blockIndices, num_storage_blocks);
  util::generate_block_id(block_id);

  // Make sure block ids are valid: 1st bit signifies EC/block_group(1)
  // or Replication/contiguous(0)
  // and last 16 bits of EC are 0 (for hierarchical naming scheme)
  ASSERT_EQ(1, nncli->is_ec_block(block_group_id));
  ASSERT_EQ(0, nncli->is_ec_block(block_id));

  uint64_t bit1_mask = (1ull << 63);  // one 1 and 63 0's
  uint64_t bit16_mask = ~(~(0ull) << 16);  // 48 0's and 16 1's
  ASSERT_EQ(0ull, (block_id) & bit1_mask);
  ASSERT_EQ(1ull, (block_group_id & bit1_mask) >> 63);
  ASSERT_EQ(0ull, block_group_id & bit16_mask);


  // Make sure we can generate block group id and index from storage block id
  for (int i = 0; i < 9; i++) {
    storage_blocks.push_back(nncli->generate_storage_block_id(
      block_group_id, i));
  }

  int i;
  for (i = 0; i < num_storage_blocks; i++) {
    ASSERT_EQ(block_group_id, nncli->get_block_group_id(storage_blocks[i]));
    ASSERT_EQ(i, nncli->get_index_within_block_group(storage_blocks[i]));
  }
}

TEST_F(StorageTest, testRecoveryTime) {
  asio::io_service io_service;
  RPCServer namenodeServer = nn_translator->getRPCServer();
  std::thread(&RPCServer::serve, namenodeServer, std::ref(io_service))
      .detach();
  sleep(3);

  ASSERT_EQ(0, system("python "
                          "/home/vagrant/rdfs/test/integration/generate_file.py"
                          " > expected_testfile1234"));
  // Put a file into rdfs.
  system(("hdfs dfs -fs hdfs://localhost:" + std::to_string(port) +
                 " -D dfs.blocksize=1048576 "
                     "-copyFromLocal expected_testfile1234 /f").c_str());
  sleep(5);

  // start a datanode to replicate to.
  initializeDatanodes(1);
  sleep(5);

  el::Loggers::setVerboseLevel(9);
  StorageMetrics metrics(zk);

  float usedBefore = metrics.usedSpace();

  // Kill an original datanode and trigger the metric measurement during repl.
  system(("pkill -f StorageTestServer" + std::to_string(minDatanodeId++))
             .c_str());

  // TODO(ejd6): revisit this for a better recovery time estimate.
  // Use StorageMetrics
  clock_t tempClock = clock();
  sleep(10);
  while (usedBefore != metrics.usedSpace()) {
    sleep(10);
  }

  LOG(INFO) << "loop done at "
            << static_cast<double>(clock() - tempClock) / CLOCKS_PER_SEC;;

  system(("hdfs dfs -fs hdfs://localhost:" + std::to_string(port) + " -rm /f")
             .c_str());
}
}  // namespace

static inline void print_usage() {
  LOG(ERROR) << "Storage test takes 0 or 1 arguments. \n"
      "-h prints this message. \n"
      "no argument runs the tests.";
}

int main(int argc, char **argv) {
  // Start up zookeeper
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh start");
  sleep(10);
  system("/home/vagrant/zookeeper/bin/zkCli.sh rmr /testing");
  sleep(5);
  system("rm -f expected_testfile1234 actual_testfile* temp* tfs*");

  // Initialize and run the tests
  ::testing::InitGoogleTest(&argc, argv);
  int res = RUN_ALL_TESTS();

  // Stop zookeeper
  system("/home/vagrant/zookeeper/bin/zkCli.sh rmr /testing");
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
  return res;
}
