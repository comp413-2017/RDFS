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

  // Initialize and run the tests
  ::testing::InitGoogleTest(&argc, argv);
  int res = RUN_ALL_TESTS();

  // Stop zookeeper
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
