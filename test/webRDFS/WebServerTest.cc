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
                "python /home/vagrant/rdfs/test/integration/generate_file.py > "
                "toDelete"));

  ASSERT_EQ(0,
            system(
                "python /home/vagrant/rdfs/test/webRDFS/generate_successful_delete.py > "
                "expectedResult"));
  system(
      "hdfs dfs -fs hdfs://localhost:5351 -D dfs.blocksize=1048576 "
      "-copyFromLocal toDelete /fileToDelete");

  ASSERT_EQ(0,
            system("curl localhost:8080/webhdfs/v1/delete/fileToDelete > result"));

  // Check that results match
  ASSERT_EQ(0,
            system("diff expectedResult result"));
  system("rm result");
  system("rm expectedRes");
}

}

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
    system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
    return res;
}
