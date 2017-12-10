// Copyright 2017 Rice University, COMP 413 2017

#include <gtest/gtest.h>
#include <easylogging++.h>
#include <thread>
#include <string>
#include <vector>
#include "../util/RDFSTestUtils.h"

using RDFSTestUtils::initializeDatanodes;

static const int NUM_DATANODES = 1;

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

void threadOneAppendF() {
  // Create an identifying text file for thread one
  system("echo 'Thread 1' > thread1.txt");
  // Append the file.
  system("hdfs dfs -fs hdfs://localhost:5351 -appendToFile thread1.txt /f");
  system("rm thread1.txt");
}

void threadTwoAppendF() {
  // Create an identifying text file for thread two
  system("echo 'Thread 2' > thread2.txt");
  // Append the file.
  system("hdfs dfs -fs hdfs://localhost:5351 -appendToFile thread2.txt /f");
  system("rm thread2.txt");
}

void threadNAppend(int threadNum) {
  // Create an identifying text file for thread threadNum
  std::string fileName = "thread" + std::to_string(threadNum) + ".txt";
  system(("echo 'Thread " + std::to_string(threadNum) + "' > "
      + fileName).c_str());
  // Append the file.
  system(("hdfs dfs -fs hdfs://localhost:5351 -appendToFile "
             + fileName + " /f" + std::to_string(threadNum)).c_str());
  system(("rm " + fileName).c_str());
}

void threadTwoAppendG() {
  // Create an identifying text file for thread two
  system("echo 'Thread 2' > thread2.txt");
  // Append the file.
  system("hdfs dfs -fs hdfs://localhost:5351 -appendToFile thread2.txt /g");
  system("rm thread2.txt");
}

namespace {

TEST(AppendFileTest, testSimpleFileAppend) {
  // Make a file.
  ASSERT_EQ(0, system(
              "python /home/vagrant/rdfs/test/integration/generate_file.py > "
                "testfile1234"));

  // Put it into rdfs.
  system(
        "hdfs dfs -fs hdfs://localhost:5351 -touchz /f");

  // Append the file.
  system(
        "hdfs dfs -fs hdfs://localhost:5351 -appendToFile testfile1234 /f");
  system(
        "hdfs dfs -fs hdfs://localhost:5351 -appendToFile testfile1234 /f");

  // Read it from rdfs.
  system("hdfs dfs -fs hdfs://localhost:5351 -cat /f > actual_testfile1234");

  // Create the expected test file by appending the test file twice
  system("cat testfile1234 > expected_testfile1234");
  system("cat testfile1234 >> expected_testfile1234");

  // Check that its contents match.
  ASSERT_EQ(0,
            system("diff expected_testfile1234 actual_testfile1234 > "
            "/dev/null"));

  // Remove files created by this test
  system("hdfs dfs -fs hdfs://localhost:5351 -rm /f");
  system("rm *testfile1234");
}

TEST(AppendFileTest, testSimpleFileSmallAppend) {
  // Make a file.
  system("echo 'helloworld' > helloworld.txt");

  // Put it into rdfs.
  system(
    "hdfs dfs -fs hdfs://localhost:5351 -touchz /f");

  // Append the file.
  system(
    "hdfs dfs -fs hdfs://localhost:5351 -appendToFile helloworld.txt /f");
  system(
    "hdfs dfs -fs hdfs://localhost:5351 -appendToFile helloworld.txt /f");

  // Read it from rdfs.
  system("hdfs dfs -fs hdfs://localhost:5351 -cat /f > actual_helloworld");

  // Create the expected test file by appending the test file twice
  system("cat helloworld.txt > expected_helloworld");
  system("cat helloworld.txt >> expected_helloworld");

  // Check that its contents match.
  ASSERT_EQ(0,
            system("diff expected_helloworld actual_helloworld > "
                     "/dev/null"));

  // Remove files created by this test
  system("hdfs dfs -fs hdfs://localhost:5351 -rm /f");
  system("rm *helloworld.txt");
}

TEST(AppendFileTest, testAppendToNonExistentFile) {
  // Make a local file.
  ASSERT_EQ(0,
            system(
              "python /home/vagrant/rdfs/test/integration/generate_file.py "
                "> testfile1234"));

  // Append the file.
  system("hdfs dfs -fs hdfs://localhost:5351 -appendToFile testfile1234 "
        "/non_existent_testfile");

  // Read it from rdfs.
  system("hdfs dfs -fs hdfs://localhost:5351 -cat /non_existent_testfile > "
        "actual_testfile1234");

  // Create the expected test file by appending the test file twice
  system("cat testfile1234 > expected_testfile1234");

  // Check that its contents match.
  ASSERT_EQ(0,
            system("diff expected_testfile1234 actual_testfile1234 > "
                  "/dev/null"));

  // Remove the created files
  system("rm testfile1234");
  system("hdfs dfs -fs hdfs://localhost:5351 -rm /non_existent_testfile");
  system("rm *testfile1234");
}

TEST(AppendFileTest, testOneClientAppends) {
  // Make a file.
  ASSERT_EQ(0,
            system(
              "python /home/vagrant/rdfs/test/integration/generate_file.py "
              "> testfile1234"));

  // Put it into rdfs.
  system("hdfs dfs -fs hdfs://localhost:5351 -copyFromLocal testfile1234 /f");

  // Create another client and append to the file from that thread
  std::thread threadOne(threadOneAppendF);
  threadOne.join();

  // Check to make sure that these append changes are in the file

  // Read it from rdfs.
  system("hdfs dfs -fs hdfs://localhost:5351 -cat /f > actual_testfile1234");

  // Create the expected test file by appending the test file twice
  system("echo 'Thread 1' > thread1.txt");
  system("cat testfile1234 > expected_testfile1234");
  system("cat thread1.txt >> expected_testfile1234");
  system("rm thread1.txt");

  // Check that its contents match.
  ASSERT_EQ(0,
            system("diff expected_testfile1234 actual_testfile1234 "
                  "> /dev/null"));

  // Remove files created by this test
  system("hdfs dfs -fs hdfs://localhost:5351 -rm /f");
  system("rm *testfile1234");
}

TEST(AppendFileTest, testTwoClientAppendToDifferentFiles) {
  // Make a file.
  ASSERT_EQ(0,
  system("python /home/vagrant/rdfs/test/integration/generate_file.py "
          "> testfile1234"));

  // Put it into rdfs.
  system("hdfs dfs -fs hdfs://localhost:5351 -copyFromLocal testfile1234 /f");
  system("hdfs dfs -fs hdfs://localhost:5351 -copyFromLocal testfile1234 /g");

// Create another client and append to the file from that thread
  std::thread threadOne(threadOneAppendF);
  std::thread threadTwo(threadTwoAppendG);
  threadOne.join();
  threadTwo.join();

  // Check to make sure that these append changes are in the file

  // Read it from rdfs.
  system("hdfs dfs -fs hdfs://localhost:5351 -cat /f > actual_testfile1234_f");
  system("hdfs dfs -fs hdfs://localhost:5351 -cat /g > actual_testfile1234_g");

  // Create the expected test file by appending the test file twice
  system("echo 'Thread 1' > thread1.txt");
  system("cat testfile1234 > expected_testfile1234_f");
  system("cat thread1.txt >> expected_testfile1234_f");
  system("rm thread1.txt");

  system("echo 'Thread 2' > thread2.txt");
  system("cat testfile1234 > expected_testfile1234_g");
  system("cat thread2.txt >> expected_testfile1234_g");
  system("rm thread2.txt");

  // Check that its contents match.
  ASSERT_EQ(0,
            system("diff expected_testfile1234_f actual_testfile1234_f "
                  "> /dev/null"));
  ASSERT_EQ(0,
            system("diff expected_testfile1234_g actual_testfile1234_g "
                  "> /dev/null"));

  // Remove files created by this test
  system("hdfs dfs -fs hdfs://localhost:5351 -rm /f");
  system("hdfs dfs -fs hdfs://localhost:5351 -rm /g");
  system("rm testfile1234");
  system("rm expected_testfile1234_*");
  system("rm actual_testfile1234_*");
}

TEST(AppendFileTest, testTwoClientAppendToSameFiles) {
  // Make a file.
  ASSERT_EQ(0,
  system("python /home/vagrant/rdfs/test/integration/generate_file.py "
          "> testfile1234"));

  // Put it into rdfs.
  system("hdfs dfs -fs hdfs://localhost:5351 -copyFromLocal testfile1234 /f");

  // Create another client and append to the file from that thread
  std::thread threadOne(threadOneAppendF);
  std::thread threadTwo(threadTwoAppendF);
  threadOne.join();
  threadTwo.join();

  // Check to make sure that these append changes are in the file

  // Read it from rdfs.
  system("hdfs dfs -fs hdfs://localhost:5351 -cat /f > actual_testfile1234_f");

  // Create the expected test file by appending the test file twice
  system("echo 'Thread 1' > thread1.txt");
  system("cat testfile1234 > thread1_expected_testfile1234_f");
  system("cat thread1.txt >> thread1_expected_testfile1234_f");

  system("echo 'Thread 2' > thread2.txt");
  system("cat testfile1234 > thread2_expected_testfile1234_f");
  system("cat thread2.txt >> thread2_expected_testfile1234_f");

  system("cat testfile1234 > thread_1_2_expected_testfile1234_f");
  system("cat thread1.txt >> thread_1_2_expected_testfile1234_f");
  system("cat thread2.txt >> thread_1_2_expected_testfile1234_f");
  system("cat testfile1234 > thread_2_1_expected_testfile1234_f");
  system("cat thread2.txt >> thread_2_1_expected_testfile1234_f");
  system("cat thread1.txt >> thread_2_1_expected_testfile1234_f");

  system("rm thread1.txt");
  system("rm thread2.txt");



  // Check that its contents match. Because only one of the threads will
  // have obtained the lease, we check to make sure that only one thread
  // successfully appended to the file.
  ASSERT_TRUE(
      system("diff thread1_expected_testfile1234_f actual_testfile1234_f "
                  "> /dev/null") == 0
        || system("diff thread2_expected_testfile1234_f actual_testfile1234_f "
                  "> /dev/null") == 0
        || system("diff thread_1_2_expected_testfile1234_f "
                  "actual_testfile1234_f > /dev/null") == 0
        || system("diff thread_2_1_expected_testfile1234_f "
                  "actual_testfile1234_f > /dev/null") == 0);

  // Remove files created by this test
  system("hdfs dfs -fs hdfs://localhost:5351 -rm /f");
  system("rm *testfile1234*");
  system("rm *expected_testfile1234_*");
  system("rm actual_testfile1234_*");
}

TEST(AppendFileTest, testNClientAppendToDifferentFiles) {
  // Make a file.
  ASSERT_EQ(0,
  system("python /home/vagrant/rdfs/test/integration/generate_file.py "
          "> testfile1234"));

  // Number of threads
  const int n = 5;

  // Put it into rdfs.
  for (int i = 0; i < n; i++) {
    system(("hdfs dfs -fs hdfs://localhost:5351 -copyFromLocal "
            "testfile1234 /f" + std::to_string(i)).c_str());
  }

  // Create another client and append to the file from that thread
  std::vector<std::thread> threads;
  for (int i = 0; i < n; i++) {
    threads.push_back(std::thread(threadNAppend, i));
  }

  for (int i = 0; i < n; i++) {
    threads[i].join();
  }

  // Check to make sure that these append changes are in the file

  // Read it from rdfs.
  for (int i = 0; i < n; i++) {
    system(("hdfs dfs -fs hdfs://localhost:5351 -cat /f" + std::to_string(i)
            + " > actual_testfile1234_f" + std::to_string(i)).c_str());
  }

  // Create the expected test file by appending the test file twice
  for (int i = 0; i < n; i++) {
    std::string threadFileName = "thread" + std::to_string(i) + ".txt";
    system(("echo 'Thread '" + std::to_string(i) + " > "
            + threadFileName).c_str());
    system(("cat testfile1234 > expected_testfile1234_f"
            + std::to_string(i)).c_str());
    system(("cat " + threadFileName + " >> expected_testfile1234_f"
            + std::to_string(i)).c_str());
    system(("rm " + threadFileName).c_str());
  }

  // Check that its contents match.
  for (int i = 0; i < n; i++) {
    ASSERT_EQ(0,
    system(("diff expected_testfile1234_f" + std::to_string(i)
            + " actual_testfile1234_f" + std::to_string(i)
            + " > /dev/null").c_str()));
  }

  // Remove files created by this test
  system("hdfs dfs -fs hdfs://localhost:5351 -rm /f*");
  system("rm testfile1234");
  system("rm actual_testfile1234_*");
  system("rm expected_testfile1234_*");
}
}  // namespace

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
  initializeDatanodes(NUM_DATANODES);
  // Initialize and run the tests
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::GTEST_FLAG(filter) =
  "AppendFileTest.testSimpleFileAppend:"
    "AppendFileTest.testSimpleFileSmallAppend";
  int res = RUN_ALL_TESTS();
  // NOTE: You'll need to scroll up a bit to see the test results

  // Remove test files and shutdown zookeeper
  system("pkill -f namenode");  // uses port 5351
  system("pkill -f AppendFileTestServer*");
  system("~/zookeeper/bin/zkCli.sh rmr /testing");
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
  return res;
}
