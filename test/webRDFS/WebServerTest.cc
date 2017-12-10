// Copyright 2017 Rice University, COMP 413 2017

#define ELPP_THREAD_SAFE

#include <easylogging++.h>

#include <gtest/gtest.h>

#include <sstream>

INITIALIZE_EASYLOGGINGPP

namespace {
TEST(WebServerTest, testDelete) {
  system("hdfs dfs -fs hdfs://comp413.local:5351 -touchz /fileToDelete");

  ASSERT_EQ(0,
            system("curl -i -X DELETE "
                   "\"https://comp413.local:8080/webhdfs/v1/"
                   "fileToDelete?user.name=vagrant&op=DELETE\" > "
                   "actualResultDelete"));

  // Check that results match
  ASSERT_EQ(0,
            system("diff /home/vagrant/rdfs/test/webRDFS/expectedResultDelete"
                  " actualResultDelete"));

  system("rm actualResultDelete");
}

TEST(WebServerTest, testRead) {
  system(
        "hdfs dfs -fs hdfs://comp413.local:5351 "
        "-copyFromLocal fileForTesting /fileToRead");

  ASSERT_EQ(0,
            system("curl -i \"https://comp413.local:8080/webhdfs/v1/"
                  "fileToRead?user.name=vagrant&op=OPEN\" > "
                  "actualResultRead"));

  // Check that results match
  ASSERT_EQ(0,
            system("diff /home/vagrant/rdfs/test/webRDFS/"
                   "expectedResultRead actualResultRead"));

  system("rm actualResultRead");
  system("hdfs dfs -fs hdfs://comp413.local:5351 -rm /fileToRead");
}

TEST(WebServerTest, testMkdir) {
  ASSERT_EQ(0,
            system("curl -i -X PUT "
                   "\"https://comp413.local:8080/webhdfs/v1/"
                   "pathToCreate?user.name=vagrant&op=MKDIRS\" "
                   "> actualResultMkdir"));

  // Check that results match
  ASSERT_EQ(0,
            system("diff /home/vagrant/rdfs/test/webRDFS/expectedResultMkdir"
                  " actualResultMkdir"));

  system("rm actualResultMkdir");
  system("hdfs dfs -fs hdfs://comp413.local:5351 -rm /pathToCreate");
}

TEST(WebServerTest, testRename) {
  system("hdfs dfs -fs hdfs://comp413.local:5351 -touchz /fileToRename");

  system("curl -i -X PUT "
                   "\"https://comp413.local:8080/webhdfs/v1/"
                   "fileToRename?user.name=vagrant&op=RENAME&destination="
                   "/newPath\" > actualResultRename");

  // Check that results match
  ASSERT_EQ(0,
            system("diff /home/vagrant/rdfs/test/webRDFS/expectedResultRename"
                   " actualResultRename"));

  system("rm actualResultRename");
  system("hdfs dfs -fs hdfs://comp413.local:5351 -rm /fileToRename");
  system("hdfs dfs -fs hdfs://comp413.local:5351 -rm /newPath");
}

TEST(WebServerTest, testFrontend) {
  ASSERT_EQ(0, system("curl -i https://comp413.local:8080 > frontend"));
  ASSERT_EQ(0, system("grep \"Content-Type: text/html\" frontend"));
  ASSERT_EQ(0, system("rm frontend"));
}

TEST(WebServerTest, testCreate) {
  system(
          "hdfs dfs -fs hdfs://comp413.local:5351 "
                  "-copyFromLocal fileForTesting /fileToCreate");

  system("curl -i -X PUT "
                   "\"https://comp413.local:8080/webhdfs/v1/"
                   "fileToCreate?user.name=vagrant&op=CREATE\" > "
                   "actualResultCreate");

  // Check that results match
  ASSERT_EQ(0,
            system("diff /home/vagrant/rdfs/test/webRDFS/"
                           "expectedResultCreate actualResultCreate"));

  system("rm actualResultCreate");
  system("hdfs dfs -fs hdfs://comp413.local:5351 -rm /fileToCreate");
}

TEST(WebServerTest, testListing) {
  system(
          "hdfs dfs -fs hdfs://comp413.local:5351 "
                  "-mkdir /dirToLs");
  system(
          "hdfs dfs -fs hdfs://comp413.local:5351 "
          "-copyFromLocal /home/vagrant/rdfs/test/webRDFS/fmakileForTesting "
           "/dirToLs/");

  ASSERT_EQ(0,
            system("curl -i \"https://comp413.local:8080/webhdfs/v1/"
                           "dirToLs?user.name=vagrant&op=LISTSTATUS\" > "
                           "actualResultLs"));

  // Check that results match except for the access times
  ASSERT_EQ(0,
            system("diff /home/vagrant/rdfs/test/webRDFS/"
                           "expectedResultLs actualResultLs"));

  system("rm actualResultLs");
  system("hdfs dfs -fs hdfs://comp413.local:5351 -rm -r /dirToLs");
}

TEST(WebServerTest, testAppend) {
    system("hdfs dfs -fs hdfs://comp413.local:5351 -touchz /fileToTestAppend");

    sleep(5);
    system("curl -i -T \"/home/vagrant/rdfs/test/webRDFS/fileForTesting\" "
           "-X POST https://comp413.local:8080/webhdfs/v1/fileToTestAppend?"
            "op=APPEND > actualResultAppend");

    // Should get same result as a read
    ASSERT_EQ(0, system("diff /home/vagrant/rdfs/test/webRDFS/"
                        "expectedResultRead actualResultAppend"));

    system("rm actualResultAppend");
    system("hdfs dfs -fs hdfs://comp413.local:5351 -rm /fileToTestAppend");
}
}  // namespace

int main(int argc, char **argv) {
  // Start up zookeeper
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh start");
  sleep(10);

  system("/home/vagrant/zookeeper/bin/zkCli.sh rmr /testing");
  system("/home/vagrant/rdfs/build/rice-namenode/namenode &");
  system("/home/vagrant/rdfs/build/rice-datanode/datanode &");
  system("/home/vagrant/rdfs/build/web-rdfs/webrdfs &");
  sleep(5);

  // Initialize and run the tests
  ::testing::InitGoogleTest(&argc, argv);
  int res = RUN_ALL_TESTS();

  // Remove test files and shutdown zookeeper
  system("hdfs dfs -fs hdfs://comp413.local:5351 -rm /fileToDelete");
  system("hdfs dfs -fs hdfs://comp413.local:5351 -rm /fileToTestAppend");
  system("pkill -f namenode");
  system("pkill -f datanode");
  system("pkill -f webrdfs");
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
  return res;
}
