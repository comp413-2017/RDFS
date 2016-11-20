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
#include "native_filesystem.h"
#include <cstring>


#include <easylogging++.h>

INITIALIZE_EASYLOGGINGPP

using asio::ip::tcp;
using client_namenode_translator::ClientNamenodeTranslator;
int num_threads = 4;
int max_xmits = 10;
namespace {

    /*
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
        std::uint64_t block_id;
        using namespace nativefs;
        NativeFS fs0("tfs0");
        NativeFS fs1("tfs1");
        NativeFS fs2("tfs2");
        block_id = fs0.getKnownBlocks()[0];
        ASSERT_EQ(block_id, fs1.getKnownBlocks()[0]);
        ASSERT_EQ(block_id, fs2.getKnownBlocks()[0]);
        std::string block0, block1, block2;
        ASSERT_TRUE(fs0.getBlock(block_id, block0));
        ASSERT_TRUE(fs1.getBlock(block_id, block1));
        ASSERT_TRUE(fs2.getBlock(block_id, block2));
        ASSERT_EQ(block0, block1);
        ASSERT_EQ(block1, block2);
    }
     */

    TEST(ReplicationTest, testReplication) {

        unsigned short xferPort = 50010;
        unsigned short ipcPort = 50020;

        ASSERT_EQ(0, system("echo 1234 > expected_testfile1234"));
        // Put it into rdfs.
        system("hdfs dfs -fs hdfs://localhost:5351 -copyFromLocal expected_testfile1234 /f");
        // Read it from rdfs.
        system("hdfs dfs -fs hdfs://localhost:5351 -cat /f > temp");
        system("head -c 5 temp > actual_testfile1234");
        // Check that its contents match.
        ASSERT_EQ(0, system("diff expected_testfile1234 actual_testfile1234"));

        // Start a new server
        std::string dnCliArgs = std::to_string(xferPort + 4) + " " + std::to_string(ipcPort + 4) + " tfs" + std::to_string(4) + " &";
        std::string cmdLine = "bash -c \"exec -a ReplicationTestServer" + std::to_string(4) + " /home/vagrant/rdfs/build/rice-datanode/datanode " +
                              dnCliArgs + "\" & ";
        system("truncate tfs4 -s 1000000000");
        system(cmdLine.c_str());

        // Kill one of the original datanodes
        system("pkill -f ReplicationTestServer0");
        sleep(20);

        // The data should now be replicated on the new server
        system("pkill -f ReplicationTestServer1");
        system("pkill -f ReplicationTestServer2");
        sleep(20);
        system("hdfs dfs -fs hdfs://localhost:5351 -cat /f > temp");
        system("head -c 5 temp > actual_testfile1234");
        ASSERT_EQ(0, system("diff expected_testfile1234 actual_testfile1234"));
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
    system("rm tfs*");
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
