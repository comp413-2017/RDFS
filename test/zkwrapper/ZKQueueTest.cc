//
// Created by Prudhvi Boyapalli on 10/17/16.
//

#include "zk_queue.h"
#include <gtest/gtest.h>

#include <easylogging++.h>

INITIALIZE_EASYLOGGINGPP

namespace {

	class ZKQueueTest : public ::testing::Test {
	protected:
		virtual void SetUp() {

			int error_code;
			// Code here will be called immediately after the constructor (right
			// before each test).

			zk = new ZKWrapper("localhost:2181", error_code);
			assert(error_code == 0); // Z_OK
			queue = new ZKQueue(*zk, "/queue_test");
		}

		// Objects declared here can be used by all tests in the test case for Foo.
		ZKWrapper *zk;
		ZKQueue *queue;
	};



	TEST_F(ZKQueueTest, Push) {
		ASSERT_EQ("/queue_test/q_item-0000000000", queue->push(ZKWrapper::EMPTY_VECTOR));
	}
}

int main(int argc, char **argv) {
	system("sudo ~/zookeeper/bin/zkServer.sh start");

	::testing::InitGoogleTest(&argc, argv);
	int res = RUN_ALL_TESTS();

	std::string command("sudo ~/zookeeper/bin/zkCli.sh rmr /queue_test");
	system(command.data());
	system("sudo ~/zookeeper/bin/zkServer.sh stop");

	return res;
}
