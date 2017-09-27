
#include "zk_dn_client.h"
#include "zk_client_common.h"
#include "zkwrapper.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <easylogging++.h>

#define ELPP_THREAD_SAFE

#include <easylogging++.h>
INITIALIZE_EASYLOGGINGPP

#define LOG_CONFIG_FILE "/home/vagrant/rdfs/config/test-log-conf.conf"

using ::testing::AtLeast;

using namespace zkclient;
class ZKDNClientTest : public ::testing::Test {
protected:
	virtual void SetUp(){
		block_id = 12345;
		block_size = 54321;
		xferPort = 50010;
		ipcPort = 50020;
		int error_code = 0;
		zk = std::make_shared<ZKWrapper>("localhost:2181", error_code, "/testing");
		ASSERT_EQ("ZOK", zk->translate_error(error_code)); // Z_OK
		client = new ZkClientDn("127.0.0.1", zk, block_size * 10, ipcPort, xferPort);
		dn_id = "127.0.0.1:50020";
	}
	virtual void TearDown() {
		system("sudo ~/zookeeper/bin/zkCli.sh rmr /testing");
	}
	uint64_t block_id;
	uint64_t block_size;
	unsigned short xferPort;
	unsigned short ipcPort;
	std::shared_ptr <ZKWrapper> zk;
	std::string dn_id;
	ZkClientDn *client;
};

TEST_F(ZKDNClientTest, RegisterMakesWorkQueues){
    bool exists;
    int error_code;

    std::string path = "/work_queues/delete/" + dn_id;

 	ASSERT_TRUE(zk->exists(path, exists, error_code));
	ASSERT_TRUE(exists);
 }


TEST_F(ZKDNClientTest, CanReadBlockSize) {
	bool exists;
	int error_code;
	uint64_t size;

	std::string path = "/block_locations/" + std::to_string(block_id);
	std::vector<std::uint8_t> data(sizeof(BlockZNode));
	zk->create(path, ZKWrapper::EMPTY_VECTOR, error_code);

	client->blockReceived(block_id, block_size);

	ASSERT_TRUE(zk->get(path, data, error_code));
	memcpy(&size, &data[0], sizeof(data));
	ASSERT_EQ(block_size, size);
}


TEST_F(ZKDNClientTest, CanDeleteBlock) {
	bool exists;
	int error_code;
	uint64_t size;

	std::string path = "/block_locations/" + std::to_string(block_id);
	std::vector<std::uint8_t> data(sizeof(BlockZNode));

	zk->create(path, ZKWrapper::EMPTY_VECTOR, error_code);

	client->blockReceived(block_id, block_size);
	ASSERT_TRUE(zk->get(path + "/" + dn_id, data, error_code));
}



int main(int argc, char **argv) {
	el::Configurations conf(LOG_CONFIG_FILE);
	el::Loggers::reconfigureAllLoggers(conf);
	el::Loggers::addFlag(el::LoggingFlag::ColoredTerminalOutput);

	system("sudo ~/zookeeper/bin/zkServer.sh start");
	system("sudo ~/zookeeper/bin/zkCli.sh rmr /testing");
	::testing::InitGoogleTest(&argc, argv);
	auto ret = RUN_ALL_TESTS();
	system("sudo ~/zookeeper/bin/zkCli.sh rmr /testing");
	system("sudo ~/zookeeper/bin/zkServer.sh stop");
	return ret;
}
