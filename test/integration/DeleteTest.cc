#include "zkwrapper.h"
#include "zk_nn_client.h"
#include "ClientNamenodeProtocolImpl.h"
#include <thread>
#include <gtest/gtest.h>


#define ELPP_THREAD_SAFE

#include <easylogging++.h>

INITIALIZE_EASYLOGGINGPP

using asio::ip::tcp;
using client_namenode_translator::ClientNamenodeTranslator;

namespace {

    class DeleteTest : public ::testing::Test {

    protected:
        virtual void SetUp() {
            int error_code;
            zk = std::make_shared<ZKWrapper>("localhost:2181", error_code, "/testing");
            assert(error_code == 0); // Z_OK

            short port = 5351;
            nncli = new zkclient::ZkNnClient(zk);
            nncli->register_watches();
            nn_translator = new ClientNamenodeTranslator(5351, *nncli);
        }

        // Objects declared here can be used by all tests in the test case for Foo.
        zkclient::ZkNnClient *nncli;
        ClientNamenodeTranslator *nn_translator;
        RPCServer *namenodeServer;
        std::shared_ptr<ZKWrapper> zk;
    };


    TEST_F(DeleteTest, testDeleteEmptyFile){
        asio::io_service io_service;
        auto namenodeServer = nn_translator->getRPCServer();
        std::thread(&RPCServer::serve, namenodeServer, std::ref(io_service)).detach();
        // Idle main thread to let the servers start up.
        // Put it into rdfs.
        system("hdfs dfs -fs hdfs://localhost:5351 -touchz /foo");
        system("hdfs dfs -fs hdfs://localhost:5351 -rm /foo");
        int error;
        bool exists;
        ASSERT_TRUE(zk->exists("/fileSystem/foo", exists, error));
        ASSERT_FALSE(exists);
    }
}

int main(int argc, char **argv) {
    // Start up zookeeper
    system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
    system("sudo /home/vagrant/zookeeper/bin/zkServer.sh start");
    // Give zk some time to start.

    // Initialize and run the tests
    ::testing::InitGoogleTest(&argc, argv);
    int res = RUN_ALL_TESTS();
    // NOTE: You'll need to scroll up a bit to see the test results

    // Remove test files and shutdown zookeeper
    system("~/zookeeper/bin/zkCli.sh rmr /testing");
    system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
    return res;
}

