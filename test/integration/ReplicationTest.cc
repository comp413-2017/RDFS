#define ELPP_THREAD_SAFE

#include "zkwrapper.h"
#include "zk_nn_client.h"
#include "zk_dn_client.h"
#include "ClientNamenodeProtocolImpl.h"
#include "data_transfer_server.h"
#include <thread>
#include <vector>
#include <gtest/gtest.h>
#include <asio.hpp>

#include <cstring>


#include <easylogging++.h>

INITIALIZE_EASYLOGGINGPP

using asio::ip::tcp;
using client_namenode_translator::ClientNamenodeTranslator;
int num_threads = 4;
int max_xmits = 10;
namespace {

    TEST(ReplicationTest, testReadWrite) {
        ASSERT_EQ(0, system("echo 1234 > expected_testfile1234"));
        // Put it into rdfs.
        system("hdfs dfs -fs hdfs://localhost:5351 -copyFromLocal expected_testfile1234 /e");
        // Read it from rdfs.
        system("hdfs dfs -fs hdfs://localhost:5351 -cat /e > temp");
        system("head -c 5 temp > actual_testfile1234");
        // Check that its contents match.
        // TODO: This test will fail until we implement the file lengths meta-data tracking.
        ASSERT_EQ(0, system("diff expected_testfile1234 actual_testfile1234"));
        sleep(20);
    }
}

int main(int argc, char **argv) {

    // Start up zookeeper
    system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
    system("sudo /home/vagrant/zookeeper/bin/zkServer.sh start");
    system("/home/vagrant/rdfs/build/rice-namenode/namenode &");
    sleep(3);
    //initialize 3 datanodes
    unsigned short xferPort = 50010;
    unsigned short ipcPort = 50020;
    for (int i = 0; i < 3; i++) {
        system(("truncate tfs" + std::to_string(i) + " -s 1000000000").c_str());
        system(("/home/vagrant/rdfs/build/rice-datanode/datanode "  + std::to_string(xferPort + i) + " " + std::to_string(ipcPort + i) + " tfs" + std::to_string(i) + " &").c_str());
        sleep(3);
    }

    // Initialize and run the tests
    ::testing::InitGoogleTest(&argc, argv);
    int res = RUN_ALL_TESTS();
    // NOTE: You'll need to scroll up a bit to see the test results

    // Remove test files and shutdown zookeeper
    /*
    system("~/zookeeper/bin/zkCli.sh rmr /testing");
    system("rm -f expected_testfile1234 actual_testfile* temp*");
    system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
     */
    return res;
}
