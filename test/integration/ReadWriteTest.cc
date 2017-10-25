// Copyright 2017 Rice University, COMP 413 2017

#include <gtest/gtest.h>
#include <easylogging++.h>
#include <thread>
#include <cstring>
#include <vector>
#include "zkwrapper.h"
#include "zk_nn_client.h"
#include "zk_dn_client.h"
#include "ClientNamenodeProtocolImpl.h"
#include "data_transfer_server.h"
#include <asio.hpp>
#include "../util/RDFSTestUtils.h"

#define ELPP_THREAD_SAFE

#define ELPP_THREAD_SAFE

INITIALIZE_EASYLOGGINGPP

using asio::ip::tcp;
using client_namenode_translator::ClientNamenodeTranslator;
using RDFSTestUtils::initializeDatanodes;

static const int NUM_DATANODES = 3;

int num_threads = 4;
int max_xmits = 2;
// These are incremented for each test.
int32_t xferPort = 50010;
int32_t ipcPort = 50020;
int maxDatanodeId = 0;
// Use minDatanodId++ when you want to kill a datanode.
int minDatanodeId = 0;

static inline void initializeDatanodes(int numDatanodes) {
  initializeDatanodes(
      maxDatanodeId,
      numDatanodes,
      "ReadWriteTestServer",
      xferPort,
      ipcPort);
  maxDatanodeId += numDatanodes;
  xferPort += numDatanodes;
  ipcPort += numDatanodes;
}

namespace {

TEST(ReadWriteTest, testReadWrite) {
  // Make a file.
  ASSERT_EQ(0,
            system(
                "python /home/vagrant/rdfs/test/integration/generate_file.py > "
                    "expected_testfile1234"));
  // Put it into rdfs.
  system(
      "hdfs dfs -fs hdfs://localhost:5351 -D dfs.blocksize=1048576 "
          "-copyFromLocal expected_testfile1234 /f");
  // Read it from rdfs.
  system("hdfs dfs -fs hdfs://localhost:5351 -cat /f > actual_testfile1234");
  // Check that its contents match.
  ASSERT_EQ(0,
            system("diff expected_testfile1234 actual_testfile1234 > "
                       "/dev/null"));
  system("hdfs dfs -fs hdfs://localhost:5351 -rm /f");
}

TEST(ReadWriteTest, testConcurrentRead) {
  // Make a file.
  ASSERT_EQ(0,
            system(
                "python /home/vagrant/rdfs/test/integration/generate_file.py >"
                    " expected_testfile1234"));
  // Put it into rdfs.
  system(
      "hdfs dfs -fs hdfs://localhost:5351 -copyFromLocal expected_testfile1234 "
          "/f");
  // Read it from rdfs.
  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; i++) {
    threads.push_back(std::thread([i]() {
      LOG(INFO) << "starting thread " << i;
      system(("hdfs dfs -fs hdfs://localhost:5351 -cat /f > temp"
          + std::to_string(i)).c_str());
      // Check that its contents match.
      ASSERT_EQ(0,
                system(("diff expected_testfile1234 temp" + std::to_string(i)
                    + " > /dev/null").c_str()));
    }));
  }
  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
  }
  system("hdfs dfs -fs hdfs://localhost:5351 -rm /f");
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

  system("/home/vagrant/rdfs/build/rice-namenode/namenode &");
  sleep(15);

  // initialize datanodes
  initializeDatanodes(3);

  // Initialize and run the tests
  ::testing::InitGoogleTest(&argc, argv);
  int res = RUN_ALL_TESTS();

  system("pkill -f namenode");  // uses port 5351
  system("pkill -f ReadWriteTestServer*");
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
  return res;
}
