#include <easylogging++.h>
#include <gtest/gtest.h>
#include "../util/RDFSTestUtils.h"

using RDFSTestUtils::initializeDatanodes;


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
      "AppendFileTestServer",
      xferPort,
      ipcPort);
  maxDatanodeId += numDatanodes;
  xferPort += numDatanodes;
  ipcPort += numDatanodes;
}

namespace {

TEST(AppendFileTest, testSimpleFileAppend) {
  // Make a file.
  ASSERT_EQ(0,
            system(
              "python /home/vagrant/rdfs/test/integration/generate_file.py > "
                "testfile1234"));

  // Put it into rdfs.
  system(
        "hdfs dfs -fs hdfs://localhost:5351 -copyFromLocal testfile1234 /f");

  // Append the file.
  system(
        "hdfs dfs -fs hdfs://localhost:5351 -appendToFile testfile1234 /f");

  // Read it from rdfs.
  system("hdfs dfs -fs hdfs://localhost:5351 -cat /f > actual_testfile1234");

  // Create the expected test file by appending the test file twice
  system("cat testfile1234 >> expected_testfile1234");
  system("cat testfile1234 >> expected_testfile1234");

  // Check that its contents match.
  ASSERT_EQ(0,
            system("diff expected_testfile1234 actual_testfile1234 > "
            "/dev/null"));

  // Remove files created by this test
  system("hdfs dfs -fs hdfs://localhost:5351 -rm /f");
  system("rm testfile1234");
  system("rm expected_testfile1234");
}

TEST(AppendFileTest, testAppendToNonExistentFile) {
  // Make a local file.
  ASSERT_EQ(0,
  system("python /home/vagrant/rdfs/test/integration/generate_file.py > testfile1234"));

  // Append the file.
  system("hdfs dfs -fs hdfs://localhost:5351 -appendToFile testfile1234 /non_existent_testfile");

  // Read it from rdfs.
  system("hdfs dfs -fs hdfs://localhost:5351 -cat /non_existent_testfile > actual_testfile1234");

  // Create the expected test file by appending the test file twice
  system("cat testfile1234 >> expected_testfile1234");

  // Check that its contents match.
  ASSERT_EQ(0,
  system("diff expected_testfile1234 actual_testfile1234 > /dev/null"));

  // Remove the created files
  system("rm testfile1234");
  system("hdfs dfs -fs hdfs://localhost:5351 -rm /non_existent_testfile");
  system("rm expected_testfile1234");
}
}

int main(int argc, char **argv) {
  // Start up zookeeper
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh start");

  // Give zk some time to start.
  sleep(10);
  system("/home/vagrant/zookeeper/bin/zkCli.sh rmr /testing");

  system("/home/vagrant/rdfs/build/rice-namenode/namenode &");
  sleep(15);

  // initialize a datanode
  initializeDatanodes(1);

  // Initialize and run the tests
  ::testing::InitGoogleTest(&argc, argv);
  int res = RUN_ALL_TESTS();
  // NOTE: You'll need to scroll up a bit to see the test results

  // Remove test files and shutdown zookeeper
  system("pkill -f namenode");  // uses port 5351
  system("pkill -f AppendFileTestServer*");
  system("~/zookeeper/bin/zkCli.sh rmr /testing");
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
  return res;
}
