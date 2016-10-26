#include "dummy-filesystem.h"
#include "dummy-filesystem.cc"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
using ::testing::AtLeast;


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

TEST_F(DummyFSTest, CanGetBlock) {
	allocateBlock(blk_id, blk);
	unsigned char* newBlock = getBlock(blk_id);
	ASSERT_EQ(newBlock[0], blk[0]);
}

TEST_F(DummyFSTest, CanRemoveBlock) {
	allocateBlock(blk_id, blk);
	ASSERT_EQ(0, rmBlock(blk_id));
}

TEST_F(DummyFSTest, RemoveNonExistBlockReturnsError) {
	ASSERT_NE(0, rmBlock(blk_id));
}


int main(int argc, char **argv) {
	::testing::InitGoogleTest(&argc, argv);
	::testing::InitGoogleMock(&argc, argv);
	return RUN_ALL_TESTS();
}
