#include "ClientNamenodeProtocolImpl.h"
#include <cstring>
#include <gtest/gtest.h>

namespace {

    class NamenodeTest : public ::testing::Test {
    protected:
        virtual void SetUp() {
        }

        virtual void TearDown() {
        }

        // Objects declared here can be used by all tests in the test case for Foo.
        ZKWrapper *zk;
    };

    
    TEST_F(NamenodeTest, create){
        ASSERT_EQ("ZCLOSING", zk->translate_error(-116));
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
