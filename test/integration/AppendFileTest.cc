#include <easylogging++.h>
#include <gtest/gtest.h>
#include "../util/RDFSTestUtils.h"

namespace {

TEST(AppendFileTest, testFileAppend) {
  // Make a file.
  ASSERT_EQ(0,
            system(
              "python /home/vagrant/rdfs/test/integration/generate_file.py > "
                "testfile1234"));

  // Put it into rdfs.
  system(
        "hdfs dfs -fs hdfs://localhost:5351 -D dfs.blocksize=1048576 "
          "-copyFromLocal testfile1234 /f");

  // Append the file.
  system(
        "hdfs dfs -fs hdfs://localhost:5351 "
          "-appendToFile testfile1234 /f");

  // Read it from rdfs.
  system("hdfs dfs -fs hdfs://localhost:5351 -cat /f > actual_testfile1234");

  // Create the expected test file by appending the test file twice
  system("cat testfile1234 >> expected_testfile1234");
  system("cat testfile1234 >> expected_testfile1234");

  // Check that its contents match.
  ASSERT_EQ(0,
            system("diff expected_testfile1234 actual_testfile1234 > "
            "/dev/null"));
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

  el::Configurations conf(LOG_CONFIG_FILE);
  el::Loggers::reconfigureAllLoggers(conf);
  el::Loggers::addFlag(el::LoggingFlag::ColoredTerminalOutput);
  el::Loggers::addFlag(el::LoggingFlag::LogDetailedCrashReason);
  el::Loggers::setVerboseLevel(9);

  // Initialize and run the tests
  ::testing::InitGoogleTest(&argc, argv);
  int res = RUN_ALL_TESTS();
  // NOTE: You'll need to scroll up a bit to see the test results

  // Remove test files and shutdown zookeeper
  system("pkill -f namenode");  // uses port 5351
  system("~/zookeeper/bin/zkCli.sh rmr /testing");
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
  return res;
}
