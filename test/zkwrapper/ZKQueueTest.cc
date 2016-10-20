//
// Created by Prudhvi Boyapalli on 10/17/16.
//

#include "zk_queue.h"
#include <gtest/gtest.h>

namespace {

    class ZKQueueTest : public ::testing::Test {
    protected:
        virtual void SetUp() {
            // Code here will be called immediately after the constructor (right
            // before each test).
            system("sudo ~/zookeeper/bin/zkServer.sh start");

            zk = new ZKWrapper("localhost:2181");
            queue = new ZKQueue(*zk, "/queue_test");
        }

        virtual void TearDown() {
            // Code here will be called immediately after each test (right
            // before the destructor).
            std::string command("sudo ~/zookeeper/bin/zkCli.sh rmr /queue_test");
            system(command.data());
            system("sudo ~/zookeeper/bin/zkServer.sh stop");
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
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
