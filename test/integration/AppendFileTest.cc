#include <easylogging++.h>
#include <gtest/gtest.h>
#include "../util/RDFSTestUtils.h"
#include <string>
#include <thread>

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
  system(("echo 'Thread" + std::to_string(threadNum) + "' > " + fileName).c_str());
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

TEST(AppendFileTest, testOneClientAppends) {
  // Make a file.
  ASSERT_EQ(0,
            system("python /home/vagrant/rdfs/test/integration/generate_file.py > testfile1234"));

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
  system("cat testfile1234 >> expected_testfile1234");
  system("cat thread1.txt >> expected_testfile1234");
  system("rm thread1.txt");

  // Check that its contents match.
  ASSERT_EQ(0,
            system("diff expected_testfile1234 actual_testfile1234 > /dev/null"));

  // Remove files created by this test
  system("hdfs dfs -fs hdfs://localhost:5351 -rm /f");
  system("rm testfile1234");
  system("rm expected_testfile1234");
}

TEST(AppendFileTest, testTwoClientAppendToDifferentFiles) {
  // Make a file.
  ASSERT_EQ(0,
  system("python /home/vagrant/rdfs/test/integration/generate_file.py > testfile1234"));

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
  system("cat testfile1234 >> expected_testfile1234_f");
  system("cat thread1.txt >> expected_testfile1234_f");
  system("rm thread1.txt");

  system("echo 'Thread 2' > thread2.txt");
  system("cat testfile1234 >> expected_testfile1234_g");
  system("cat thread2.txt >> expected_testfile1234_g");
  system("rm thread2.txt");

  // Check that its contents match.
  ASSERT_EQ(0,
            system("diff expected_testfile1234_f actual_testfile1234_f > /dev/null"));
  ASSERT_EQ(0,
            system("diff expected_testfile1234_g actual_testfile1234_g > /dev/null"));

  // Remove files created by this test
  system("hdfs dfs -fs hdfs://localhost:5351 -rm /f");
  system("hdfs dfs -fs hdfs://localhost:5351 -rm /g");
  system("rm testfile1234_*");
  system("rm expected_testfile1234_*");
}

TEST(AppendFileTest, testNClientAppendToDifferentFiles) {
  // Make a file.
  ASSERT_EQ(0,
  system("python /home/vagrant/rdfs/test/integration/generate_file.py > testfile1234"));

  // Number of threads
  int n = 5;

  // Put it into rdfs.
  for (int i = 0; i < n; i++) {
    system("hdfs dfs -fs hdfs://localhost:5351 -copyFromLocal testfile1234 /f" + i);
  }

  // Create another client and append to the file from that thread
  std::thread threads[n];
  for (int i = 0; i < n; i++) {
    threads[i] = std::thread(threadNAppend, i);
  }

  for (int i = 0; i < n; i++) {
    threads[i].join();
  }

  // Check to make sure that these append changes are in the file

  // Read it from rdfs.
  for (int i = 0; i < n; i++) {
    system(("hdfs dfs -fs hdfs://localhost:5351 -cat /f" + std::to_string(i) + " > actual_testfile1234_f" + std::to_string(i)).c_str());
  }

  // Create the expected test file by appending the test file twice
  for (int i = 0; i < n; i++) {
    std::string threadFileName = "thread" + std::to_string(i) + ".txt";
    system(("echo 'Thread '" + std::to_string(i) + " > " + threadFileName).c_str());
    system(("cat testfile1234 >> expected_testfile1234_f" + std::to_string(i)).c_str());
    system(("cat " + threadFileName + " >> expected_testfile1234_f" + std::to_string(i)).c_str());
    system(("rm " + threadFileName).c_str());
  }

  // Check that its contents match.
  for (int i = 0; i < n; i++) {
    ASSERT_EQ(0,
    system(("diff expected_testfile1234_f" + std::to_string(i) + " actual_testfile1234_f" + std::to_string(i) + " > /dev/null").c_str()));
  }

  // Remove files created by this test
  system("hdfs dfs -fs hdfs://localhost:5351 -rm /f*");
  system("rm testfile1234_*");
  system("rm expected_testfile1234_*");
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
