#include "zkwrapper.h"
#include <gtest/gtest.h>

namespace {

    class ZKWrapperTest : public ::testing::Test {
    protected:
        virtual void SetUp() {
            // Code here will be called immediately after the constructor (right
            // before each test).
            system("sudo ~/zookeeper/bin/zkServer.sh start");
            int error_code;
            zk = new ZKWrapper("localhost:2181", error_code);
            assert(error_code == 0); // Z_OK
        }

        virtual void TearDown() {
            // Code here will be called immediately after each test (right
            // before the destructor).
            std::string command("sudo ~/zookeeper/bin/zkCli.sh rmr /testing");
            system(command.data());
            system("sudo ~/zookeeper/bin/zkServer.sh stop");
        }

        static int preTest(ZKWrapper& zk) {
            int error_code;
            bool exists;
            assert(zk.exists("/testing", exists, error_code));
            if (exists) {
                assert(zk.recursive_delete("/testing/", error_code));
                // assert(zk.get_children("/testing", 0).size() == 0);
            } else {
                assert(zk.create("/testing", ZKWrapper::EMPTY_VECTOR, error_code));
            }

        }

        // Objects declared here can be used by all tests in the test case for Foo.
        ZKWrapper *zk;
    };

    TEST_F(ZKWrapperTest, RecursiveDelete) {
        preTest(* zk);

        int error_code;

        zk->create("/testing/child1", ZKWrapper::EMPTY_VECTOR, error_code);
        zk->create("/testing/child1/child2", ZKWrapper::EMPTY_VECTOR, error_code);
        zk->create("/testing/child1/child3", ZKWrapper::EMPTY_VECTOR, error_code);

        ASSERT_TRUE(zk->recursive_delete("/testing/child1", error_code));

        bool exists;
        ASSERT_TRUE(zk->exists("/testing/child1", exists, error_code));
        ASSERT_EQ(false, exists);

        zk->create("/testing/child1", ZKWrapper::EMPTY_VECTOR, error_code);
        zk->create("/testing/child2", ZKWrapper::EMPTY_VECTOR, error_code);
        zk->create("/testing/child1/child1", ZKWrapper::EMPTY_VECTOR, error_code);
        zk->create("/testing/child1/child2", ZKWrapper::EMPTY_VECTOR, error_code);
        zk->create("/testing/child2/child1", ZKWrapper::EMPTY_VECTOR, error_code);

        zk->recursive_delete("/testing/", error_code);

        auto children = std::vector<std::string>();
        ASSERT_TRUE(zk->get_children("/testing", children, error_code));
        ASSERT_EQ(0, children.size());

    }

    TEST_F(ZKWrapperTest, MultiOperation) {
        preTest(* zk);

        int error_code;

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

        std::vector <std::uint8_t> vec = std::vector <std::uint8_t>();

        zk->get("/testing/child1", vec, error_code);
        ASSERT_EQ(hello_vec, vec);
        zk->get("/testing/child2", vec, error_code);
        ASSERT_EQ(jello_vec, vec);
        zk->get("/testing/toDelete", vec, error_code);
        ASSERT_EQ(bye_vec, vec);

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

        zk->get("/testing/child1", vec, error_code);
        ASSERT_EQ(nhello_vec, vec);
        zk->get("/testing/child2", vec, error_code);
        ASSERT_EQ(njello_vec, vec);

        bool exists;
        assert(zk->exists("/testing/toDelete", exists, error_code));
        ASSERT_TRUE(!exists);
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
