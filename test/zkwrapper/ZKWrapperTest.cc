#include "zkwrapper.h"
#include <gtest/gtest.h>

namespace {

    class ZKWrapperTest : public ::testing::Test {
    protected:
        virtual void SetUp() {
            // Code here will be called immediately after the constructor (right
            // before each test).
            system("sudo ~/zookeeper/bin/zkServer.sh start");

            zk = new ZKWrapper("localhost:2181");
        }

        virtual void TearDown() {
            // Code here will be called immediately after each test (right
            // before the destructor).
            std::string command("sudo ~/zookeeper/bin/zkCli.sh rmr /testing");
            system(command.data());
            system("sudo ~/zookeeper/bin/zkServer.sh stop");
        }

        static int preTest(ZKWrapper& zk) {
            if (zk.exists("/testing", 0)) {
                zk.recursive_delete("/testing/");
                assert(zk.get_children("/testing", 0).size() == 0);
            } else {
                zk.create("/testing", ZKWrapper::EMPTY_VECTOR);
            }
        }

        // Objects declared here can be used by all tests in the test case for Foo.
        ZKWrapper *zk;
    };

    TEST_F(ZKWrapperTest, RecursiveDelete) {
        preTest(* zk);
        zk->create("/testing/child1", ZKWrapper::EMPTY_VECTOR);
        zk->create("/testing/child1/child2", ZKWrapper::EMPTY_VECTOR);
        zk->create("/testing/child1/child3", ZKWrapper::EMPTY_VECTOR);

        zk->recursive_delete("/testing/child1");
        ASSERT_EQ(false, zk->exists("/testing/child1", 0));

        zk->create("/testing/child1", ZKWrapper::EMPTY_VECTOR);
        zk->create("/testing/child2", ZKWrapper::EMPTY_VECTOR);
        zk->create("/testing/child1/child1", ZKWrapper::EMPTY_VECTOR);
        zk->create("/testing/child1/child2", ZKWrapper::EMPTY_VECTOR);
        zk->create("/testing/child2/child1", ZKWrapper::EMPTY_VECTOR);

        zk->recursive_delete("/testing/");
        ASSERT_EQ(0, zk->get_children("/testing", 0).size());
    }

    TEST_F(ZKWrapperTest, MultiOperation) {
        preTest(* zk);

        auto hello_vec = ZKWrapper::get_byte_vector("hello");
        auto jello_vec = ZKWrapper::get_byte_vector("jello");
        auto bye_vec = ZKWrapper::get_byte_vector("bye");
        auto op = zk->build_create_op("/testing/child1", hello_vec);
        auto op2 = zk->build_create_op("/testing/child2", jello_vec);
        auto op3 = zk->build_create_op("/testing/toDelete", bye_vec);

        auto operations = std::vector<std::shared_ptr<ZooOp>>();

        operations.push_back(op);
        operations.push_back(op2);
        operations.push_back(op3);

        std::vector<zoo_op_result> results = std::vector<zoo_op_result>();
        zk->execute_multi(operations, results);

        ASSERT_EQ(hello_vec, zk->get("/testing/child1", 0));
        ASSERT_EQ(jello_vec, zk->get("/testing/child2", 0));
        ASSERT_EQ(bye_vec, zk->get("/testing/toDelete", 0));

        auto nhello_vec = ZKWrapper::get_byte_vector("new_hello");
        auto njello_vec = ZKWrapper::get_byte_vector("new_jello");
        auto op4 = zk->build_set_op("/testing/child1", nhello_vec);
        auto op5 = zk->build_set_op("/testing/child2", njello_vec);
        auto op6 = zk->build_delete_op("/testing/toDelete");

        operations = std::vector<std::shared_ptr<ZooOp>>();

        operations.push_back(op4);
        operations.push_back(op5);
        operations.push_back(op6);

        zk->execute_multi(operations, results);

        ASSERT_EQ(nhello_vec, zk->get("/testing/child1", 0));
        ASSERT_EQ(njello_vec, zk->get("/testing/child2", 0));
        ASSERT_TRUE(!zk->exists("/toDelete", 0));
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
