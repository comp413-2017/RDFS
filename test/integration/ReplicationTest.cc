// Copyright 2017 Rice University, COMP 413 2017

#include <easylogging++.h>
#include <gtest/gtest.h>

#include <thread>
#include <cstring>
#include <vector>
#include <asio.hpp>
#include <StorageMetrics.h>

#include "zkwrapper.h"
#include "zk_nn_client.h"
#include "zk_dn_client.h"
#include "ClientNamenodeProtocolImpl.h"
#include "data_transfer_server.h"
#include "native_filesystem.h"
#include "../util/RDFSTestUtils.h"

#define ELPP_THREAD_SAFE

INITIALIZE_EASYLOGGINGPP

using asio::ip::tcp;
using client_namenode_translator::ClientNamenodeTranslator;
using RDFSTestUtils::initializeDatanodes;

static const int NUM_DATANODES = 3;

int num_threads = 4;
int max_xmits = 100000;
// These are incremented for each test.
int32_t xferPort = 50010;
int32_t ipcPort = 50020;
int maxDatanodeId = 0;
// Use minDatanodId++ when you want to kill a datanode.
int minDatanodeId = 0;
uint16_t nextPort = 5351;

static inline void initializeDatanodes(int numDatanodes) {
  initializeDatanodes(
      maxDatanodeId,
      numDatanodes,
      "ReplicationTestServer",
      xferPort,
      ipcPort);
  maxDatanodeId += numDatanodes;
  xferPort += numDatanodes;
  ipcPort += numDatanodes;
}

namespace {

class ReplicationTest : public ::testing::Test {
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
    system("pkill -f ReplicationTestServer*");
  }

  // Objects declared here can be used by all tests below.
  static zkclient::ZkNnClient *nncli;
  static ClientNamenodeTranslator *nn_translator;
  static std::shared_ptr<ZKWrapper> zk;
  unsigned short port;
};

zkclient::ZkNnClient *ReplicationTest::nncli = NULL;
ClientNamenodeTranslator *ReplicationTest::nn_translator = NULL;
std::shared_ptr<ZKWrapper> ReplicationTest::zk = NULL;

TEST_F(ReplicationTest, testReadWrite) {
  asio::io_service io_service;
  RPCServer namenodeServer = nn_translator->getRPCServer();
  std::thread(&RPCServer::serve, namenodeServer, std::ref(io_service))
      .detach();
  sleep(3);

  ASSERT_EQ(0, system("python /home/vagrant/rdfs/test/integration/"
                          "generate_file.py > expected_testfile1234"));
  // Put it into rdfs.
  system(("hdfs dfs -fs hdfs://localhost:" + std::to_string(port) +
      " -D dfs.blocksize=1048576 "
             "-copyFromLocal expected_testfile1234 /f").c_str());
  // Read it from rdfs.
  system(("hdfs dfs -fs hdfs://localhost:"+ std::to_string(port) +
      " -cat /f > "
             "actual_testfile1234").c_str());
  // Check that its contents match.
  ASSERT_EQ(0, system("diff expected_testfile1234 actual_testfile1234 > "
                          "/dev/null"));

  sleep(10);
  using nativefs::NativeFS;
  NativeFS fs0("tfs0");
  NativeFS fs1("tfs1");
  NativeFS fs2("tfs2");
  auto block_ids = fs0.getKnownBlocks();
  for (auto block_id : block_ids) {
    std::string block0, block1, block2;
    ASSERT_TRUE(fs0.getBlock(block_id, block0));
    ASSERT_TRUE(fs1.getBlock(block_id, block1));
    ASSERT_TRUE(fs2.getBlock(block_id, block2));
    ASSERT_EQ(block0, block1);
    ASSERT_EQ(block1, block2);
  }
  system(("hdfs dfs -fs hdfs://localhost:" + std::to_string(port) +
      " -rm /f").c_str());
}

TEST_F(ReplicationTest, testReplication) {
  asio::io_service io_service;
  RPCServer namenodeServer = nn_translator->getRPCServer();
  std::thread(&RPCServer::serve, namenodeServer, std::ref(io_service))
      .detach();
  sleep(3);

  ASSERT_EQ(0, system("python /home/vagrant/rdfs/test/integration/"
                          "generate_file.py > expected_testfile1234"));
  // Put it into rdfs.
  sleep(10);
  system(("hdfs dfs -fs hdfs://localhost:" + std::to_string(port) +
      " -D dfs.blocksize=1048576 "
          "-copyFromLocal expected_testfile1234 /g").c_str());
  // Read it from rdfs.
  sleep(10);
  system(("hdfs dfs -fs hdfs://localhost:" + std::to_string(port) +
      " -cat /g > actual_testfile1234").c_str());
  // Check that its contents match.
  sleep(10);
  ASSERT_EQ(0,
            system("diff expected_testfile1234 actual_testfile1234 > "
                       "/dev/null"));

  // Start a new DataNode
  initializeDatanodes(1);

  sleep(10);
  // Kill one of the original datanodes
  system(("pkill -f ReplicationTestServer" + std::to_string(minDatanodeId++))
             .c_str());
  sleep(10);

  // The file should still be readable.
  sleep(10);
  system(("hdfs dfs -fs hdfs://localhost:" + std::to_string(port) +
      " -cat /g > actual_testfile12345").c_str());
  ASSERT_EQ(0,
            system("diff expected_testfile1234 actual_testfile12345 > "
                       "/dev/null"));

  system(("hdfs dfs -fs hdfs://localhost:" + std::to_string(port) +
      " -rm /g").c_str());
}

TEST_F(ReplicationTest, testReplicationOnFailure) {
  asio::io_service io_service;
  RPCServer namenodeServer = nn_translator->getRPCServer();
  std::thread(&RPCServer::serve, namenodeServer, std::ref(io_service))
      .detach();
  sleep(3);


  ASSERT_EQ(0, system("python /home/vagrant/rdfs/test/integration/"
                          "generate_file.py > expected_testfile1234"));
  // Put it into rdfs with the 3 original DataNodes.
  sleep(10);
  system(("hdfs dfs -fs hdfs://localhost:" + std::to_string(port) +
      " -D dfs.blocksize=1048576 "
             "-copyFromLocal expected_testfile1234 /g").c_str());
  // Read it from rdfs.
  sleep(10);
  system(("hdfs dfs -fs hdfs://localhost:" + std::to_string(port) +
      " -cat /g > actual_testfile1234").c_str());
  // Check that its contents match.
  sleep(10);
  ASSERT_EQ(0,
            system("diff expected_testfile1234 actual_testfile1234 > "
                       "/dev/null"));

  system("rm -f expected_testfile1234 actual_testfile*");

  initializeDatanodes(3);

  StorageMetrics metrics(zk);
  float usedBefore = metrics.usedSpace();

  // Kill 3 original datanodes
  int i = 0;
  for (; i < 3; i++) {
    system(("pkill -f ReplicationTestServer" + std::to_string(minDatanodeId++))
               .c_str());
    sleep(10);
  }

  // The data should now be replicated on the new servers.
  ASSERT_EQ(usedBefore, metrics.usedSpace());

  system(("hdfs dfs -fs hdfs://localhost:" + std::to_string(port) +
      " -rm /g").c_str());
}
}  // namespace

int main(int argc, char **argv) {
  // Start up zookeeper
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh start");
  sleep(10);
  system("/home/vagrant/zookeeper/bin/zkCli.sh rmr /testing");
  sleep(3);
  system("rm -f expected_testfile1234 actual_testfile* temp* tfs*");
  sleep(3);

  // Initialize and run the tests
  ::testing::InitGoogleTest(&argc, argv);
  int res = RUN_ALL_TESTS();

  // NOTE: You'll need to scroll up a bit to see the test results
  // Remove test files and shutdown zookeeper
  system("/home/vagrant/zookeeper/bin/zkCli.sh rmr /testing");
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
  return res;
}
