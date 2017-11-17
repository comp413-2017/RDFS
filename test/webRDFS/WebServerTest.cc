// Copyright 2017 Rice University, COMP 413 2017

#define ELPP_THREAD_SAFE

#include <easylogging++.h>

#include <gtest/gtest.h>
#include "webRequestTranslator.h"

#include <iostream>

INITIALIZE_EASYLOGGINGPP

namespace {
TEST(WebServerTest, testDelete) {
  system("hdfs dfs -fs hdfs://localhost:5351 -touchz /fileToDelete");

  ASSERT_EQ(0,
            system("curl -i --insecure https://localhost:8080/webhdfs/v1/"
                  "fileToDelete?op=DELETE > actualResultDelete"));

  // Check that results match
  ASSERT_EQ(0,
            system("diff /home/vagrant/rdfs/test/webRDFS/expectedResultDelete"
                  " actualResultDelete"));

  system("rm actualResultDelete");
}

TEST(WebServerTest, testRead) {
  system(
        "hdfs dfs -fs hdfs://localhost:5351 "
        "-copyFromLocal fileForTesting /fileToRead");

  ASSERT_EQ(0,
            system("curl -i --insecure https://localhost:8080/webhdfs/v1/"
                  "fileToRead?op=OPEN > actualResultRead"));

  // Check that results match
  ASSERT_EQ(0,
            system("diff /home/vagrant/rdfs/test/webRDFS/"
                   "expectedResultRead actualResultRead"));

  system("rm actualResultRead");
  system("hdfs dfs -fs hdfs://localhost:5351 -rm /fileToRead");
}

TEST(WebServerTest, testCreate) {
  system(
        "hdfs dfs -fs hdfs://localhost:5351 "
        "-copyFromLocal fileForTesting /fileToCreate");

      ASSERT_EQ(0,
                system("curl -i --insecure https://localhost:8080/webhdfs/v1/"
                               "fileToCreate?op=CREATE > actualResultCreate"));

      // Check that results match
      ASSERT_EQ(0,
                system("diff /home/vagrant/rdfs/test/webRDFS/"
                               "expectedResultCreate actualResultCreate"));

      system("rm actualResultCreate");
      system("hdfs dfs -fs hdfs://localhost:5351 -rm /fileToCreate");
}

TEST(WebServerTest, testListing) {
  system(
         "hdfs dfs -fs hdfs://localhost:5351 "
         "-mkdir /dirToLs");
  system(
        "hdfs dfs -fs hdfs://localhost:5351 "
        "-copyFromLocal /home/vagrant/rdfs/test/webRDFS/fileForTesting /dirToLs/");

      ASSERT_EQ(0,
                system("curl -i --insecure https://localhost:8080/webhdfs/v1/"
                               "dirToLs?op=LISTSTATUS > actualResultLs"));

      // Check that results match
      ASSERT_EQ(0,
                system("diff /home/vagrant/rdfs/test/webRDFS/"
                               "expectedResultLs actualResultLs"));

      //system("rm actualResultLs");
      system("hdfs dfs -fs hdfs://localhost:5351 -rm -r /dirToLs");
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
  system("hdfs dfs -fs hdfs://localhost:5351 -rm /fileToDelete");
  system("pkill -f namenode");
  system("pkill -f datanode");
  system("pkill -f webrdfs");
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
  return res;
}
