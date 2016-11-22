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
		ASSERT_EQ(0, system("python /home/vagrant/rdfs/test/integration/generate_file.py > expected_testfile1234"));
		// Put it into rdfs.
		system("hdfs dfs -fs hdfs://localhost:5351 -copyFromLocal expected_testfile1234 /e");
		// Read it from rdfs.
		system("hdfs dfs -fs hdfs://localhost:5351 -cat /e > actual_testfile1234");
		// Check that its contents match.
		// TODO: This test will fail until we implement the file lengths meta-data tracking.
		ASSERT_EQ(0, system("diff expected_testfile1234 actual_testfile1234 > /dev/null"));
	}

	TEST(ReadWriteTest, testConcurrentRead){
		// Make a file.
		ASSERT_EQ(0, system("python /home/vagrant/rdfs/test/integration/generate_file.py > expected_testfile1234"));
		// Put it into rdfs.
		system("hdfs dfs -fs hdfs://localhost:5351 -copyFromLocal expected_testfile1234 /f");
		// Read it from rdfs.
		std::vector<std::thread> threads;
		for (int i = 0; i < num_threads; i++){

			threads.push_back(std::thread([i](){
				LOG(INFO) << "starting thread " << i;
				system(("hdfs dfs -fs hdfs://localhost:5351 -cat /f > temp" + std::to_string(i)).c_str());
				// Check that its contents match.
				// TODO: This test will fail until we implement the file lengths meta-data tracking.
				ASSERT_EQ(0, system(("diff expected_testfile1234 temp"+ std::to_string(i) + " > /dev/null").c_str()));
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
	system("~/zookeeper/bin/zkCli.sh rmr /testing");
	system("rm -f _RW_TEST_FS expected_testfile1234 actual_testfile* temp*");
	system("truncate _RW_TEST_FS -s 1000000000");

	system("/home/vagrant/rdfs/build/rice-namenode/namenode &");
	sleep(5);
	system("/home/vagrant/rdfs/build/rice-datanode/datanode 50010 50020 _RW_TEST_FS &");
	sleep(10);
	// Initialize and run the tests
	::testing::InitGoogleTest(&argc, argv);
	int res = RUN_ALL_TESTS();

	system("pkill -f namenode");
	system("pkill -f datanode");
	system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
	return res;
}

