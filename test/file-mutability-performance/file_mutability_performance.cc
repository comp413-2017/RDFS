// Copyright 2017 Rice University, COMP 413 2017
// NOTE: Please download and configure HDFS before running
// https://hadoop.apache.org/docs/current/hadoop-project-dist +
//   /hadoop-common/SingleCluster.html

#include <easylogging++.h>
#include <math.h>
#include <chrono>
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

double performAppend(std::string hdfsPath, int appendSize, int numAppends) {
  std::mt19937 gen{ std::random_device()() };
  std::uniform_int_distribution<> dis(0, 255);
  std::ofstream file("appendFile.txt");
  std::generate_n(std::ostream_iterator<char>(file, ""),
                  appendSize, [&]{ return dis(gen); });

  system((hdfsPath + " dfs -touchz /a").c_str());
  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < numAppends; i++) {
    system((hdfsPath + " dfs -appendToFile appendFile.txt /a").c_str());
  }
  auto finish = std::chrono::high_resolution_clock::now();
  system((hdfsPath + " dfs -rm /a").c_str());
  std::chrono::duration<double> elapsed = finish - start;
  std::cout << "Elapsed time: " << elapsed.count() << " s\n";
  return elapsed.count();
}

void trackAppendSizePerformance(std::string hdfsPath,
                                std::string outputFileName) {
  int maxPower = 6;
  int coefficients[3] = {1, 3, 5};

  std::ofstream outputFile;
  outputFile.open(outputFileName);
  outputFile << "Performing appends on varying append sizes:\n";
  for (int i = 0; i < maxPower; i++) {
    for (const int &coefficient : coefficients) {
      int appendSize = coefficient * pow(10, i);
      double time = performAppend(hdfsPath, appendSize, 1);
      outputFile << appendSize << " bytes: " << time << " seconds\n";
    }
  }
  outputFile.close();
}

void performRDFSAnalysis() {
  std::string path = "hdfs";
  std::string outputFileName = "rdfsAppendPerformanceResults.txt";
  trackAppendSizePerformance(path, outputFileName);
}

void performHDFSAnalysis() {
  std::string path = "$HOME/hadoop-2.9.0/bin/hdfs";
  std::string outputFileName = "hdfsAppendPerformanceResults.txt";
  trackAppendSizePerformance(path, outputFileName);
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
  initializeDatanodes(NUM_DATANODES);

  performRDFSAnalysis();

  // Remove test files and shutdown zookeeper
  system("pkill -f namenode");  // uses port 5351
  system("pkill -f AppendFileTestServer*");
  system("~/zookeeper/bin/zkCli.sh rmr /testing");
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");

  // Run HDFS
  system("$HOME/hadoop-2.9.0/sbin/start-dfs.sh");
  performHDFSAnalysis();
  system("$HOME/hadoop-2.9.0/sbin/stop-dfs.sh");

  int res = 0;
  return res;
}
