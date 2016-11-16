#include "native_filesystem.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <easylogging++.h>
INITIALIZE_EASYLOGGINGPP
using ::testing::AtLeast;

using namespace nativefs;
class NativeFSTest : public ::testing::Test {
	public:
		NativeFSTest() : filesystem("/dev/sdb") {}
protected:
	virtual void SetUp() {
		blk = "here's some data to write in the block";
	}
	std::string blk;
	NativeFS filesystem;
};

// TODO: Test writing and getting multiple blocks

TEST_F(NativeFSTest, CanWriteBlock) {
	ASSERT_EQ(true, filesystem.writeBlock(1, blk));
}

TEST_F(NativeFSTest, CanGetBlock) {
	bool write_success = filesystem.writeBlock(2, blk);
	ASSERT_EQ(true, write_success);
	bool success;
	std::string newBlock = filesystem.getBlock(2, success);
	ASSERT_EQ(true, success);
	ASSERT_EQ(blk[0], newBlock[0]);
}

TEST_F(NativeFSTest, CanRemoveBlock) {
	ASSERT_EQ(true, filesystem.rmBlock(1));
	ASSERT_EQ(true, filesystem.rmBlock(2));
}

TEST_F(NativeFSTest, RemoveNonExistBlockReturnsError) {
	ASSERT_EQ(false, filesystem.rmBlock(3));
}

int main(int argc, char **argv) {
	::testing::InitGoogleTest(&argc, argv);
	::testing::InitGoogleMock(&argc, argv);
	int result = RUN_ALL_TESTS();
	return result;
}
