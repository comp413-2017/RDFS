#include "native_filesystem.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <easylogging++.h>
INITIALIZE_EASYLOGGINGPP
using ::testing::AtLeast;

using namespace nativefs;
class NativeFSTest : public ::testing::Test {
	public:
		NativeFSTest() : filesystem("NATIVEFSTESTFS") {}
protected:
	virtual void SetUp() {
		blk_id = 1;
		blk = "here's some data to write in the block";
	}
	long blk_id;
	std::string blk;
	NativeFS filesystem;
};


TEST_F(NativeFSTest, CanWriteBlock) {
	ASSERT_EQ(true, filesystem.writeBlock(blk_id, blk));
}

TEST_F(NativeFSTest, CanGetBlock) {
	bool write_success = filesystem.writeBlock(blk_id, blk);
	ASSERT_EQ(true, write_success);
	bool success;
	std::string newBlock = filesystem.getBlock(blk_id, success);
	ASSERT_EQ(blk[0], newBlock[0]);
	ASSERT_EQ(success, true);
}

TEST_F(NativeFSTest, CanRemoveBlock) {
	filesystem.writeBlock(blk_id, blk);
	ASSERT_EQ(true, filesystem.rmBlock(blk_id));
}

TEST_F(NativeFSTest, RemoveNonExistBlockReturnsError) {
	ASSERT_EQ(false, filesystem.rmBlock(blk_id));
}

int main(int argc, char **argv) {
	::testing::InitGoogleTest(&argc, argv);
	::testing::InitGoogleMock(&argc, argv);
	return RUN_ALL_TESTS();
}
