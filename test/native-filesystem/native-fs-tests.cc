#include "native-filesystem.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
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


TEST_F(NativeFSTest, CanAllocateBlock) {
	ASSERT_EQ(true, filesystem.allocateBlock(blk_id, blk));
}

TEST_F(NativeFSTest, CanGetBlock) {
	filesystem.allocateBlock(blk_id, blk);
	unsigned char* newBlock = filesystem.getBlock(blk_id);
	ASSERT_EQ(newBlock[0], blk[0]);
}

TEST_F(NativeFSTest, CanRemoveBlock) {
	filesystem.allocateBlock(blk_id, blk);
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
