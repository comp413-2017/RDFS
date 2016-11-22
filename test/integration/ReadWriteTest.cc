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
int max_xmits = 2;
namespace {

	TEST(ReadWriteTest, testReadWrite){
		// Make a file.
		ASSERT_EQ(0, system("echo 1234 > expected_testfile1234"));
		// Put it into rdfs.
		system("hdfs dfs -fs hdfs://localhost:5351 -copyFromLocal expected_testfile1234 /e");
		// Read it from rdfs.
		system("hdfs dfs -fs hdfs://localhost:5351 -cat /e > actual_testfile1234");
		// Check that its contents match.
		// TODO: This test will fail until we implement the file lengths meta-data tracking.
		ASSERT_EQ(0, system("diff expected_testfile1234 actual_testfile1234"));
	}

	TEST(ReadWriteTest, testConcurrentRead){
		// Make a file.
		ASSERT_EQ(0, system("echo 1234 > expected_testfile1234"));
		// Put it into rdfs.
		system("hdfs dfs -fs hdfs://localhost:5351 -copyFromLocal expected_testfile1234 /e");
		// Read it from rdfs.
		std::vector<std::thread> threads;
		for (int i = 0; i < num_threads; i++){

			threads.push_back(std::thread([i](){
				LOG(INFO) << "starting thread " << i;
				system(("hdfs dfs -fs hdfs://localhost:5351 -cat /e > actual_testfile" + std::to_string(i)).c_str());
				// Check that its contents match.
				// TODO: This test will fail until we implement the file lengths meta-data tracking.
				ASSERT_EQ(0, system(("diff expected_testfile1234 actual_testfile"+ std::to_string(i)).c_str()));
			}));
		}
		for (int i = 0; i < num_threads; i++){
			threads[i].join();
		}
	}
}

int main(int argc, char **argv) {
	// Start up zookeeper
	system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
	system("sudo /home/vagrant/zookeeper/bin/zkServer.sh start");
	system("truncate _RW_TEST_FS -s 1000000000");

	zkclient::ZkNnClient *nncli;
	ClientNamenodeTranslator *nn_translator;
	std::shared_ptr<zkclient::ZkClientDn> dncli;
	TransferServer *dn_transfer_server;
	int error_code;
	auto zk_shared = std::make_shared<ZKWrapper>("localhost:2181", error_code, "/testing");
	assert(error_code == 0); // Z_OK

	unsigned short xferPort = 50010;
	unsigned short ipcPort = 50020;
	auto fs = std::make_shared<nativefs::NativeFS>("_RW_TEST_FS");
	dncli = std::make_shared<zkclient::ZkClientDn>("127.0.0.1", zk_shared, ipcPort, xferPort);
	dn_transfer_server = new TransferServer(xferPort, fs, dncli, max_xmits);

	sleep(3);

	short port = 5351;
	nncli = new zkclient::ZkNnClient(zk_shared);
	nncli->register_watches();
	nn_translator = new ClientNamenodeTranslator(5351, *nncli);

	asio::io_service io_service;
	auto namenodeServer = nn_translator->getRPCServer();
	std::thread(&TransferServer::serve, dn_transfer_server, std::ref(io_service)).detach();
	std::thread(&RPCServer::serve, namenodeServer, std::ref(io_service)).detach();
	sleep(5);

	// Initialize and run the tests
	::testing::InitGoogleTest(&argc, argv);
	int res = RUN_ALL_TESTS();
	// NOTE: You'll need to scroll up a bit to see the test results

	// Remove test files and shutdown zookeeper
	system("~/zookeeper/bin/zkCli.sh rmr /testing");
	system("rm -f _RW_TEST_FS expected_testfile1234 actual_testfile* temp*");
	system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
	return res;
}

