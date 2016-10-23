// #include "mock_turtle.h"
// #include "painter.h"
// #include "test_func.cc"
#include "dummy-filesystem.h"
#include "dummy-filesystem.cc"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
using ::testing::AtLeast;

// Example tests
// TEST(PainterTest, CanDrawSomething) {
//   MockTurtle turtle;
//   EXPECT_CALL(turtle, PenDown())
//     .Times(AtLeast(1));
//
//   Painter painter(&turtle);
//
//   EXPECT_TRUE(painter.DrawCircle(0, 0, 10));
// }
//
// TEST(MyTest, BlahBlah) {
// 	ASSERT_EQ(6, squareRoot(36));
// }

class DummyFSTest : public ::testing::Test {
        protected:
                virtual void SetUp(){
                        blk_id = 0;
                        blk = (unsigned char*) malloc(sizeof(unsigned char)*5);
                        for(int i = 0; i < 5; i++)
                        {
                                blk[i] = 'X';
                        }
                }
                long blk_id;
                unsigned char* blk;
};


TEST_F(DummyFSTest, CanAllocateBlock) {
        ASSERT_EQ(0, allocateBlock(blk_id, blk));
}

TEST_F(DummyFSTest, CanAddBlock) {
        allocateBlock(blk_id, blk);
        ASSERT_EQ(blk, getBlock(blk_id));
}

TEST_F(DummyFSTest, CanRemoveBlock) {
        ASSERT_EQ(0, 0);
	//ASSERT_EQ(0, rmBlock(blk_id));
}


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    return RUN_ALL_TESTS();
}
