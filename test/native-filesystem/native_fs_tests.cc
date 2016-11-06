#include "native_filesystem.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <easylogging++.h>
INITIALIZE_EASYLOGGINGPP
using ::testing::AtLeast;

using namespace nativefs;
class NativeFSTest : public ::testing::Test {
protected:
	virtual void SetUp(){
		blk_id = 0;
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
	filesystem.writeBlock(blk_id, blk);
	bool success;
	std::string newBlock = filesystem.getBlock(blk_id, success);
	ASSERT_EQ(newBlock[0], blk[0]);
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
