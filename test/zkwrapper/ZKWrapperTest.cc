#include "zkwrapper.h"
#include <cstring>
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

    
    TEST_F(ZKWrapperTest, translate_error){
        ASSERT_EQ("ZCLOSING", zk->translate_error(-116));
    }
    TEST_F(ZKWrapperTest, create){
        preTest(* zk);
        int error = 0;
        bool result = zk->create("/testing/testcreate", ZKWrapper::EMPTY_VECTOR, error);
        ASSERT_EQ(true, result);
        ASSERT_EQ("ZOK", zk->translate_error(error));

        result = zk->create("/testing/testcreate", ZKWrapper::EMPTY_VECTOR, error);

        ASSERT_EQ(false, result);
        ASSERT_EQ("ZNODEEXISTS", zk->translate_error(error));
    }
    TEST_F(ZKWrapperTest, create_sequential){
        preTest(* zk);
        std::string new_path;
        int error = 0;
        bool result = zk->create_sequential("/testing/sequential-", ZKWrapper::EMPTY_VECTOR, new_path, false, error);
        ASSERT_EQ(true, result);
        ASSERT_EQ("ZOK", zk->translate_error(error));
        std::string expected("/testing/sequential-0000000000"); 
		ASSERT_EQ(expected, new_path);

        std::string new_path2;
        result = zk->create_sequential("/testing/sequential-", ZKWrapper::EMPTY_VECTOR, new_path2, false, error);

        ASSERT_EQ(true, result);
        ASSERT_EQ("ZOK", zk->translate_error(error));
        std::string expected2("/testing/sequential-0000000001");
		ASSERT_EQ(expected2, new_path2);

    }
    TEST_F(ZKWrapperTest, recursive_create){
        preTest(* zk);
        int error = 0;
        bool result = zk->recursive_create("/testing/testrecur/test1", ZKWrapper::EMPTY_VECTOR, error);
        ASSERT_EQ(true, result);
        ASSERT_EQ("ZOK", zk->translate_error(error));
		
		/* create same path, should fail */
		result = zk->recursive_create("/testing/testrecur/test1", ZKWrapper::EMPTY_VECTOR, error);
		ASSERT_EQ(false, result);
	
	   std::vector <std::uint8_t> retrieved_data(65536);
	   auto data = ZKWrapper::get_byte_vector("hello");
       result = zk->recursive_create("/testing/testrecur/test2", data, error);
	   ASSERT_EQ(true, result);
	   result = zk->get("/testing/testrecur/test2", retrieved_data, error);
       ASSERT_EQ(true, result);
	   ASSERT_EQ(5, retrieved_data.size());
	   result = zk->get("/testing/testrecur", retrieved_data, error);
	   ASSERT_EQ(true, result);
	   ASSERT_EQ(0, retrieved_data.size());		

        result = zk->recursive_create("/testing/testrecur/test2/test3/test4", ZKWrapper::EMPTY_VECTOR, error);
        ASSERT_EQ(true, result);
        ASSERT_EQ("ZOK", zk->translate_error(error));
    }
    TEST_F(ZKWrapperTest, exists){
        preTest(* zk);
        int error = 0;
        bool exist = false;

        bool result = zk->create("/testing/testcreate", ZKWrapper::EMPTY_VECTOR, error);
        ASSERT_EQ(true, result);
        ASSERT_EQ("ZOK", zk->translate_error(error));

        result = zk->exists("/testing/testcreate", exist, error);
        ASSERT_EQ(true, result);
        ASSERT_EQ(true, exist);
        ASSERT_EQ("ZOK", zk->translate_error(error));

        result = zk->exists("/testing/not_exists", exist, error);
        ASSERT_EQ(true, result);
        ASSERT_EQ(false, exist);
        ASSERT_EQ("ZNONODE", zk->translate_error(error));
    }

    //TODO need to create tests for this
    TEST_F(ZKWrapperTest, wexists){
        ASSERT_EQ("ZCLOSING", zk->translate_error(-116));
    }
    
    TEST_F(ZKWrapperTest, get_children){
        preTest(* zk);
        int error = 0;
        std::vector <std::string> children;
        bool result = zk->get_children("/", children, error);
        ASSERT_EQ(true, result);
        ASSERT_EQ("ZOK", zk->translate_error(error));
        //ASSERT_EQ(1, children.size());
    }

    //TODO need to create tests for this
    TEST_F(ZKWrapperTest, wget_children){
        ASSERT_EQ("ZCLOSING", zk->translate_error(-116));
    }
    TEST_F(ZKWrapperTest, get){
        preTest(* zk);
        int error = 0;
        bool result = zk->create("/testing/testget", ZKWrapper::EMPTY_VECTOR, error);
        ASSERT_EQ(true, result);
        ASSERT_EQ("ZOK", zk->translate_error(error));

        std::vector <std::uint8_t> data(65536);
        result = zk->get("/testing/testget", data, error);
        ASSERT_EQ(true, result);
        ASSERT_EQ("ZOK", zk->translate_error(error));
        ASSERT_EQ(0, data.size());

        auto data_1 = ZKWrapper::get_byte_vector("hello");

        result = zk->create("/testing/testget_withdata", data_1, error);
        ASSERT_EQ(true, result);
        ASSERT_EQ("ZOK", zk->translate_error(error));

        std::vector <std::uint8_t> retrieved_data(65536);
        result = zk->get("/testing/testget_withdata", retrieved_data, error);
        ASSERT_EQ(true, result);
        ASSERT_EQ("ZOK", zk->translate_error(error));
        ASSERT_EQ(5, retrieved_data.size());
    }
    //TODO need to create tests for this
    TEST_F(ZKWrapperTest, wget){
        ASSERT_EQ("ZCLOSING", zk->translate_error(-116));
    }
    TEST_F(ZKWrapperTest, set){
        preTest(* zk);
        int error = 0;
        auto data = ZKWrapper::get_byte_vector("hello");

        bool result = zk->create("/testing/testget", ZKWrapper::EMPTY_VECTOR, error);
        ASSERT_EQ(true, result);
        ASSERT_EQ("ZOK", zk->translate_error(error));

        result = zk->set("/testing/testget", data, error);
        ASSERT_EQ(true, result);
        ASSERT_EQ("ZOK", zk->translate_error(error));

        std::vector <std::uint8_t> data_get(65536);
        result = zk->get("/testing/testget", data_get, error);
        ASSERT_EQ(true, result);
        ASSERT_EQ("ZOK", zk->translate_error(error));
        ASSERT_EQ(5, data_get.size());
    }

    TEST_F(ZKWrapperTest, delete_node){
        preTest(* zk);
        int error = 0;

        bool result = zk->create("/testing/testcreate", ZKWrapper::EMPTY_VECTOR, error);
        ASSERT_EQ(true, result);
        ASSERT_EQ("ZOK", zk->translate_error(error));

        result = zk->delete_node("/testing/testcreate", error);
        ASSERT_EQ(true, result);
        ASSERT_EQ("ZOK", zk->translate_error(error));

        result = zk->delete_node("/testing/not_exists", error);
        ASSERT_EQ(false, result);
        ASSERT_EQ("ZNONODE", zk->translate_error(error));
    }
    
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
