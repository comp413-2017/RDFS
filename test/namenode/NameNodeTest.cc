#include "zkwrapper.h"
#include "zk_nn_client.h"

#include <gtest/gtest.h>

#include <cstring>
#include <easylogging++.h>

INITIALIZE_EASYLOGGINGPP

namespace {

	class NamenodeTest : public ::testing::Test {

		protected:
			virtual void SetUp() {
				int error_code;
				auto zk_shared = std::make_shared<ZKWrapper>("localhost:2181", error_code, "/testing");
				assert(error_code == 0); // Z_OK
				client = new zkclient::ZkNnClient(zk_shared);
				zk = new ZKWrapper("localhost:2181", error_code, "/testing");
			}

			// Objects declared here can be used by all tests in the test case for Foo.
			ZKWrapper *zk;
			zkclient::ZkNnClient *client;
	};


	TEST_F(NamenodeTest, checkNamespace){
		// nuffin
	}

	TEST_F(NamenodeTest, findDataNodes){

		int error;
		zk->create("/health/localhost:2181", ZKWrapper::EMPTY_VECTOR, error);
		zk->create("/health/localhost:2181/health", ZKWrapper::EMPTY_VECTOR, error);
		zk->create("/health/localhost:2182", ZKWrapper::EMPTY_VECTOR, error);
		zk->create("/health/localhost:2182/health", ZKWrapper::EMPTY_VECTOR, error);

		auto datanodes = std::vector<std::string>();
		u_int64_t block_id;
		LOG(INFO) << "Finding dn's for block " << block_id;
		util::generate_uuid(block_id);
		client->find_datanode_for_block(datanodes, block_id, 1, true);

		for (auto datanode : datanodes) {
			LOG(INFO) << "Returned datanode " << datanode;
		}
		ASSERT_EQ(1, datanodes.size());
	}

	TEST_F(NamenodeTest, findDataNodesWithReplicas){
		// Check if we can find datanodes, without overlapping with ones that already contain a replica
	}

	TEST_F(NamenodeTest, basicCheckAcks){
		// Check if check_acks works as intended
		int error;
		zk->delete_node("/work_queues/wait_for_acks/block_uuid_1/dn-id-3", error);
		zk->delete_node("/work_queues/wait_for_acks/block_uuid_1/dn-id-2", error);
		zk->delete_node("/work_queues/wait_for_acks/block_uuid_1/dn-id-1", error);
		zk->delete_node("/work_queues/wait_for_acks/block_uuid_1", error);

		auto data = std::vector<std::uint8_t>();
		data.push_back(3);
		zk->create("/work_queues/wait_for_acks/block_uuid_1", data, error, false);
		ASSERT_EQ(0, error);

		zk->create("/work_queues/wait_for_acks/block_uuid_1/dn-id-1", ZKWrapper::EMPTY_VECTOR, error);
		ASSERT_EQ(0, error);

		// Only one DN acknowledged, but not timed out, so should succeed
		ASSERT_EQ(true, client->check_acks());

		zk->create("/work_queues/wait_for_acks/block_uuid_1/dn-id-2", ZKWrapper::EMPTY_VECTOR, error);
		ASSERT_EQ(0, error);
		// Only two DNs acknowledged, but not timed out, so should succeed
		ASSERT_EQ(true, client->check_acks());

		zk->create("/work_queues/wait_for_acks/block_uuid_1/dn-id-3", ZKWrapper::EMPTY_VECTOR, error);
		ASSERT_EQ(0, error);
		// All three DNs acknowledged, so should succeed
		ASSERT_EQ(true, client->check_acks());


		// Since enough DNs acknowledged, the block_uuid_1 should have been removed from wait_for_acks
		auto children = std::vector<std::string>();
		zk->get_children("/work_queues/wait_for_acks", children, error);
		ASSERT_EQ(0, children.size());
	}

	TEST_F(NamenodeTest, testRenameFile){
		int error_code;
		zk->create("/fileSystem/old_name", zk->get_byte_vector("File data"), error_code, false);
		ASSERT_EQ(0, error_code);

		std::string new_path;
		zk->create_sequential("/fileSystem/old_name/block-", zk->get_byte_vector("Block uuid"), new_path, false, error_code);
		ASSERT_EQ(0, error_code);
		ASSERT_EQ("/fileSystem/old_name/block-0000000000", new_path);

		ASSERT_TRUE(client->rename_file("/old_name", "/new_name"));

		auto new_file_data = std::vector<std::uint8_t>();
		zk->get("/fileSystem/new_name", new_file_data, error_code);
		ASSERT_EQ(0, error_code);
		ASSERT_EQ("File data", std::string(new_file_data.begin(), new_file_data.end()));
		LOG(INFO) << "New: " << std::string(new_file_data.begin(), new_file_data.end());

		auto new_block_data = std::vector<std::uint8_t>();
		zk->get("/fileSystem/new_name/block-0000000000", new_block_data, error_code);
		ASSERT_EQ(0, error_code);
		ASSERT_EQ("Block uuid", std::string(new_block_data.begin(), new_block_data.end()));
		LOG(INFO) << "New: " << std::string(new_block_data.begin(), new_block_data.end());

		// Ensure that old_name was delete
		bool exist;
		zk->exists("/fileSystem/old_name", exist, error_code);
		ASSERT_EQ(false, exist);
	}

	TEST_F(NamenodeTest, previousBlockComplete){
		int error;
		u_int64_t block_id;
		util::generate_uuid(block_id);
		LOG(INFO) << "Previous block_id is " << block_id;
		ASSERT_EQ(false, client->previousBlockComplete(block_id));
		/* mock the directory */
		zk->create("/block_locations", ZKWrapper::EMPTY_VECTOR, error);
		zk->create("/block_locations/"+std::to_string(block_id), ZKWrapper::EMPTY_VECTOR, error);
		ASSERT_EQ(false, client->previousBlockComplete(block_id));
		/* mock the child directory */
		zk->create("/block_locations/"+std::to_string(block_id)+"/child1", ZKWrapper::EMPTY_VECTOR, error);
		ASSERT_EQ(true, client->previousBlockComplete(block_id));
	}

}

int main(int argc, char **argv) {
	// Start up zookeeper
	system("sudo ~/zookeeper/bin/zkServer.sh stop");
	system("sudo ~/zookeeper/bin/zkServer.sh start");

	// Initialize and run the tests
	::testing::InitGoogleTest(&argc, argv);
	int res = RUN_ALL_TESTS();
	// NOTE: You'll need to scroll up a bit to see the test results

	// Remove test files and shutdown zookeeper
	system("sudo ~/zookeeper/bin/zkCli.sh rmr /testing");
	system("sudo ~/zookeeper/bin/zkServer.sh stop");
	return res;
}
