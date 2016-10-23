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

TEST(DummyFSTest, CanAllocateBlock) {
	long blk_id;
	unsigned char* blk = (unsigned char*) malloc(sizeof(unsigned char)*5);
	for(int i = 0; i < 5; i++)
    {
        blk[i] = 'X';
    }
	ASSERT_EQ(0, allocateBlock(0, blk));
}


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    return RUN_ALL_TESTS();
}
