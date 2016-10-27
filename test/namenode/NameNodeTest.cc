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
        }

        virtual void TearDown() {
        }

        // Objects declared here can be used by all tests in the test case for Foo.
        ZKWrapper *zk;
        zkclient::ZkNnClient *client;
    };

    
    TEST_F(NamenodeTest, create){
        // ASSERT_EQ("ZCLOSING", zk->translate_error(-116));
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
