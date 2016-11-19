//
// Created by Nicholas Kwon on 11/19/16.
//

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
    }
}

int main(int argc, char **argv) {
    asio::io_service io_service;

    // Start up zookeeper
    system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
    system("sudo /home/vagrant/zookeeper/bin/zkServer.sh start");
    int error_code;
    auto zk_shared = std::make_shared<ZKWrapper>("localhost:2181", error_code, "/testing");
    assert(error_code == 0); // Z_OK

    // initialize namenode
    zkclient::ZkNnClient *nncli;
    ClientNamenodeTranslator *nn_translator;
    nncli = new zkclient::ZkNnClient(zk_shared);
    nncli->register_watches();
    nn_translator = new ClientNamenodeTranslator(5351, *nncli);
    auto namenodeServer = nn_translator->getRPCServer();
    std::thread(&RPCServer::serve, namenodeServer, std::ref(io_service)).detach();
    sleep(3);


    //initialize 3 datanodes
    unsigned short xferPort = 50010;
    unsigned short ipcPort = 50020;

    for (int i = 0; i < 3; i++) {
        std::shared_ptr <zkclient::ZkClientDn> dncli;
        TransferServer *dn_transfer_server;
        system(("truncate tfs" + std::to_string(i) + " -s 1000000000").c_str());
        auto fs = std::make_shared<nativefs::NativeFS>("tfs" + std::to_string(i));
        dncli = std::make_shared<zkclient::ZkClientDn>("127.0.0.1", zk_shared, ipcPort + i, xferPort + i);
        dn_transfer_server = new TransferServer(xferPort + i, fs, dncli, max_xmits);
        std::thread(&TransferServer::serve, dn_transfer_server, std::ref(io_service)).detach();
    }
    sleep(5);

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
