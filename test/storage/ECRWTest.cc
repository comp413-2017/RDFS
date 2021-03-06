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

static const int NUM_DATANODES = 9;

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
      "ECRWTestServer",
      xferPort,
      ipcPort,
      true /* output to files */);
  maxDatanodeId += numDatanodes;
  xferPort += numDatanodes;
  ipcPort += numDatanodes;
}

namespace {

class ECRWTest : public ::testing::Test {
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
    system("pkill -f ECRWTestServer*");
  }

  // Objects declared here can be used by all tests below.
  zkclient::ZkNnClient *nncli;
  ClientNamenodeTranslator *nn_translator;
  std::shared_ptr<ZKWrapper> zk;
  unsigned short port;
};

/**
 * basic EC RW test
 */
TEST_F(ECRWTest, testECRW) {
  asio::io_service io_service;
  RPCServer namenodeServer = nn_translator->getRPCServer();
  std::thread(&RPCServer::serve, namenodeServer, std::ref(io_service)).detach();
  sleep(3);

  ASSERT_EQ(0, system("python /home/vagrant/rdfs/test/integration/"
                          "generate_file.py > expected_""testfile1234"));

  // Set the EC policy first
  system("hdfs ec -setPolicy -path / -policy RS-6-3-1024k");

  // Put a file into rdfs.
  system(("hdfs dfs -fs hdfs://localhost:" +
      std::to_string(port) +
          " -copyFromLocal CMakeCache.txt /f").c_str());
  sleep(30);

  // Read it from rdfs.
  system("hdfs dfs -fs hdfs://localhost:5351 -ls /");
  system("hdfs dfs -fs hdfs://localhost:5351 -cat /f > actual_testfile1234");
  // Check that its contents match.
//  ASSERT_EQ(0,
//            system("diff expected_testfile1234 actual_testfile1234 > "
//                       "/dev/null"));
}
}  // namespace

// don't add more tests to this file please.

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
  system("rm -f expected_testfile1234 datanode*_out actual_testfile* temp* "
             "tfs*");

  // Initialize and run the tests
  ::testing::InitGoogleTest(&argc, argv);
  int res = RUN_ALL_TESTS();

  // Stop zookeeper
  system("/home/vagrant/zookeeper/bin/zkCli.sh rmr /testing");
  system("pkill -f ECRWTestServer*");
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
  return res;
}
