// Copyright 2017 Rice University, COMP 413 2017

#define ELPP_THREAD_SAFE

#include <easylogging++.h>

#include <gtest/gtest.h>

#include <iostream>

INITIALIZE_EASYLOGGINGPP

namespace {
TEST(WebServerTest, testDelete) {
  system("hdfs dfs -fs hdfs://localhost:5351 -touchz /fileToDelete");

  ASSERT_EQ(0,
            system("curl -i -X DELETE --insecure "
                   "https://localhost:8080/webhdfs/v1/"
                   "fileToDelete?op=DELETE > /home/vagrant/rdfs/test/webRDFS"
                   "/actualResultDelete"));

  // Check that results match
  ASSERT_EQ(0,
            system("diff /home/vagrant/rdfs/test/webRDFS/expectedResultDelete"
                  " /home/vagrant/rdfs/test/webRDFS/actualResultDelete"));

  system("rm /home/vagrant/rdfs/test/webRDFS/actualResultDelete");
}

TEST(WebServerTest, testRead) {
  system(
        "hdfs dfs -fs hdfs://localhost:5351 "
        "-copyFromLocal fileForTesting /fileToRead");

  ASSERT_EQ(0,
            system("curl -i --insecure https://localhost:8080/webhdfs/v1/"
                  "fileToRead?op=OPEN > /home/vagrant/rdfs/test/webRDFS"
                  "/actualResultRead"));

  // Check that results match
  ASSERT_EQ(0,
            system("diff /home/vagrant/rdfs/test/webRDFS/"
                   "expectedResultRead /home/vagrant/rdfs/test/webRDFS"
                   "/actualResultRead"));

  system("rm /home/vagrant/rdfs/test/webRDFS/actualResultRead");
  system("hdfs dfs -fs hdfs://localhost:5351 -rm /fileToRead");
}

TEST(WebServerTest, testMkdir) {
  ASSERT_EQ(0,
            system("curl -i -X PUT --insecure "
                   "https://localhost:8080/webhdfs/v1/"
                   "pathToCreate?op=MKDIRS > /home/vagrant/rdfs/test/webRDFS"
                   "/actualResultMkdir"));

  // Check that results match
  ASSERT_EQ(0,
            system("diff /home/vagrant/rdfs/test/webRDFS/expectedResultMkdir"
                   " /home/vagrant/rdfs/test/webRDFS/actualResultMkdir"));

  system("rm /home/vagrant/rdfs/test/webRDFS/actualResultMkdir");
  system("hdfs dfs -fs hdfs://localhost:5351 -rm /pathToCreate");
}

TEST(WebServerTest, testRename) {
  system("hdfs dfs -fs hdfs://localhost:5351 -touchz /fileToRename");

  system("curl -i -X PUT --insecure "
                   "\"https://localhost:8080/webhdfs/v1/"
                   "fileToRename?op=RENAME&destination=newPath\" > "
                   "/home/vagrant/rdfs/test/webRDFS/actualResultRename");

  // Check that results match
  ASSERT_EQ(0,
            system("diff /home/vagrant/rdfs/test/webRDFS/expectedResultRename"
                   " /home/vagrant/rdfs/test/webRDFS/actualResultRename"));

  system("rm /home/vagrant/rdfs/test/webRDFS/actualResultRename");
  system("hdfs dfs -fs hdfs://localhost:5351 -rm /fileToRename");
}

}  // namespace

int main(int argc, char **argv) {
  // Start up zookeeper
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh start");
  sleep(10);

  system("sudo /home/vagrant/zookeeper/bin/zkCli.sh rmr /testing");
  sleep(3);

  system("/home/vagrant/rdfs/build/rice-namenode/namenode &");
  sleep(10);
  system("/home/vagrant/rdfs/build/rice-datanode/datanode &");
  sleep(10);
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
  system("sudo /home/vagrant/zookeeper/bin/zkCli.sh rmr /testing");
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
  return res;
}
