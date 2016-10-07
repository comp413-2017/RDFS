#include "mock_turtle.h"
#include "painter.h"
#include "test_func.cc"
#include <gmock/gmock.h>
#include <gtest/gtest.h>

TEST(PainterTest, CanDrawSomething) {
  MockTurtle turtle;                          

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
