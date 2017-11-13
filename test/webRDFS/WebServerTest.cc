// Copyright 2017 Rice University, COMP 413 2017

#define ELPP_THREAD_SAFE

#include <easylogging++.h>

#include <gtest/gtest.h>
#include "webRequestTranslator.h"

#include <iostream>

INITIALIZE_EASYLOGGINGPP

namespace {
TEST(WebServerTest, testDelete) {
  ASSERT_EQ(0,
            system(
                "python /home/vagrant/rdfs/test/integration/generate_file.py "
                "> toDelete"));

  ASSERT_EQ(0,
            system(
            "python /home/vagrant/rdfs/test/webRDFS/"
            "generate_successful_delete.py > expectedResultDelete"));
  system(
      "hdfs dfs -fs hdfs://localhost:5351 -D dfs.blocksize=1048576 "
      "-copyFromLocal toDelete /fileToDelete");

  ASSERT_EQ(0,
            system("curl --insecure https://localhost:8080/webhdfs/v1/"
                  "fileToDelete?op=DELETE > actualResultDelete"));

  // Check that results match
  ASSERT_EQ(0, system("diff expectedResultDelete actualResultDelete"));
  system("rm actualResultDelete");
  system("rm expectedResultDelete");
}

TEST(WebServerTest, testRead) {
  ASSERT_EQ(0,
            system("python /home/vagrant/rdfs/test/webRDFS"
            "/generate_read_file.py > toRead"));

  ASSERT_EQ(0,
            system(
                "python /home/vagrant/rdfs/test/webRDFS"
                "/generate_successful_read.py > expectedResultRead"));

  system(
        "hdfs dfs -fs hdfs://localhost:5351 -D dfs.blocksize=1048576 "
        "-copyFromLocal toRead /fileToRead");

  ASSERT_EQ(0,
            system("curl --insecure https://localhost:8080/webhdfs/v1/fileToRead"
                  "?op=OPEN > actualResultRead"));

  // Check that results match
  ASSERT_EQ(0, system("diff expectedResultRead actualResultRead"));
  system("rm actualResultRead");
  system("rm expectedResultRead");
  system("hdfs dfs -fs hdfs://localhost:5351 -rm /fileToRead");
}

}  // namespace

int main(int argc, char **argv) {
  // Start up zookeeper
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh start");
  sleep(10);

  system("/home/vagrant/rdfs/build/rice-namenode/namenode &");
  system("/home/vagrant/rdfs/build/rice-datanode/datanode &");
  system("/home/vagrant/rdfs/build/web-rdfs/webrdfs &");
  sleep(5);

  // Initialize and run the tests
  ::testing::InitGoogleTest(&argc, argv);
  int res = RUN_ALL_TESTS();

  // Remove test files and shutdown zookeeper
  system("~/zookeeper/bin/zkCli.sh rmr /testing");
  system("hdfs dfs -fs hdfs://localhost:5351 -rm /fileToDelete");
  system("pkill -f namenode");
  system("pkill -f datanode");
  system("pkill -f webrdfs");
  system("rm toRead");
  system("rm toDelete");
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
  return res;
}
