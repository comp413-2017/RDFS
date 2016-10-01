#include "test_func.cc"
#include <gtest/gtest.h>

TEST(MyTest, BlahBlah) {
	ASSERT_EQ(6, squareRoot(36.0));
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
