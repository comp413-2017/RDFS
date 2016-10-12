#include "mock_turtle.h"
#include "painter.h"
#include "test_func.cc"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
using ::testing::AtLeast;

TEST(PainterTest, CanDrawSomething) {
  MockTurtle turtle;                          
  EXPECT_CALL(turtle, PenDown())
    .Times(AtLeast(1));

  Painter painter(&turtle);                   

  EXPECT_TRUE(painter.DrawCircle(0, 0, 10));
}

TEST(MyTest, BlahBlah) {
	ASSERT_EQ(6, squareRoot(36));
}                                             


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    return RUN_ALL_TESTS();
}
