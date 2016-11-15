#include "zkwrapper.h"
#include "zk_nn_client.h"
#include "zk_dn_client.h"
#include "ClientNamenodeProtocolImpl.h"
#include "data_transfer_server.h"
#include <thread>

#include <gtest/gtest.h>
#include <asio.hpp>

#include <cstring>

#define ELPP_THREAD_SAFE

#include <easylogging++.h>

INITIALIZE_EASYLOGGINGPP

using asio::ip::tcp;
using client_namenode_translator::ClientNamenodeTranslator;

namespace {

	class ReadWriteTest : public ::testing::Test {

	protected:
		virtual void SetUp() {
			int error_code;
			auto zk_shared = std::make_shared<ZKWrapper>("localhost:2181", error_code, "/testing");
			assert(error_code == 0); // Z_OK

			short port = 5351;
			nncli = new zkclient::ZkNnClient(zk_shared);
			nncli->register_watches();
			nn_translator = new ClientNamenodeTranslator(5351, *nncli);
			sleep(3);

			unsigned short xferPort = 50010;
			unsigned short ipcPort = 50020;
			dncli = new zkclient::ZkClientDn("127.0.0.1", "localhost", zk_shared, ipcPort, xferPort);
			nativefs::NativeFS fs;
			dn_transfer_server = new TransferServer(xferPort, fs, *dncli);
			// Give the datanode a second to register itself on the /health index.

		}

		// Objects declared here can be used by all tests in the test case for Foo.
		zkclient::ZkNnClient *nncli;
		ClientNamenodeTranslator *nn_translator;
		zkclient::ZkClientDn *dncli;
		RPCServer *namenodeServer;
		TransferServer *dn_transfer_server;
	};


	TEST_F(ReadWriteTest, testReadWrite){
		asio::io_service io_service;
		auto namenodeServer = nn_translator->getRPCServer();
		std::thread(&TransferServer::serve, dn_transfer_server, std::ref(io_service)).detach();
		std::thread(&RPCServer::serve, namenodeServer, std::ref(io_service)).detach();
		// Idle main thread to let the servers start up.
		sleep(3);
		// Make a file.
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
	// Start up zookeeper
	system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
	system("sudo /home/vagrant/zookeeper/bin/zkServer.sh start");
	// Give zk some time to start.
	sleep(5);

	// Initialize and run the tests
	::testing::InitGoogleTest(&argc, argv);
	int res = RUN_ALL_TESTS();
	// NOTE: You'll need to scroll up a bit to see the test results

	// Remove test files and shutdown zookeeper
	system("rm -f expected_testfile1234 actual_testfile1234 temp");
	system("~/zookeeper/bin/zkCli.sh rmr /testing");
	system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
	return res;
}

