// Copyright 2017 Rice University, COMP 413 2017

#include <easylogging++.h>
#include <gtest/gtest.h>
#include <zk_nn_client.h>
#include "ClientNamenodeProtocolImpl.h"

#include "StorageMetrics.h"

#define ELPP_THREAD_SAFE

INITIALIZE_EASYLOGGINGPP

using asio::ip::tcp;
using client_namenode_translator::ClientNamenodeTranslator;

static const int NUM_DATANODES = 3;

int32_t xferPort = 50010;
int32_t ipcPort = 50020;
int maxDatanodeId = 0;
int minDatanodeId = 0;

static inline void initializeDatanodes(int numDatanodes) {
  int i = maxDatanodeId;
  maxDatanodeId += numDatanodes;
  for (; i < maxDatanodeId; i++) {
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
  xferPort += numDatanodes;
  ipcPort += numDatanodes;
}

namespace {

class StorageTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
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

  virtual void SetUp() {
    initializeDatanodes(NUM_DATANODES);
  }

  virtual void TearDown() {
    system("pkill -f StorageTestServer*");
    system("hdfs dfs -fs hdfs://localhost:5351 -rm /f");
  }

  // Objects declared here can be used by all tests below.
  static zkclient::ZkNnClient *nncli;
  static ClientNamenodeTranslator *nn_translator;
  static std::shared_ptr<ZKWrapper> zk;
};

zkclient::ZkNnClient *StorageTest::nncli = NULL;
ClientNamenodeTranslator *StorageTest::nn_translator = NULL;
std::shared_ptr<ZKWrapper> StorageTest::zk = NULL;

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
