// Copyright 2017 Rice University, COMP 413 2017

#include "native_filesystem.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <easylogging++.h>
INITIALIZE_EASYLOGGINGPP
using ::testing::AtLeast;

using nativefs::NativeFS;
using nativefs::MAX_BLOCK_SIZE;
using nativefs::MIN_BLOCK_SIZE;
using nativefs::DISK_SIZE;
using nativefs::RESERVED_SIZE;

class NativeFSTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    blk = "here's some data to write in the block";
    backing = "_NATIVEFS_TEST_FS";
    backing_smaller = backing + "2";
  }
  std::string blk;
  std::string backing;
  std::string backing_smaller;
};

TEST_F(NativeFSTest, CanWriteBlock) {
  NativeFS filesystem(backing);
  ASSERT_TRUE(filesystem.writeBlock(1, blk));
}

TEST_F(NativeFSTest, WriteExistingBlock) {
  NativeFS filesystem(backing);
  filesystem.writeBlock(1, blk);
  ASSERT_FALSE(filesystem.writeBlock(1, blk));
}

TEST_F(NativeFSTest, CanWriteAndGetBlock) {
  NativeFS filesystem(backing);
  bool write_success = filesystem.writeBlock(2, blk);
  ASSERT_TRUE(write_success);
  std::string newBlock;
  ASSERT_TRUE(filesystem.hasBlock(2));
  bool success = filesystem.getBlock(2, newBlock);
  ASSERT_TRUE(success);
  ASSERT_EQ(0, blk.compare(0, blk.length(), newBlock));
}

TEST_F(NativeFSTest, CanWriteAndGetMultipleBlocks) {
  NativeFS filesystem(backing);
  bool write_success_1 = filesystem.writeBlock(10, blk);
  bool write_success_2 = filesystem.writeBlock(11, blk);
  bool write_success_3 = filesystem.writeBlock(12, blk);
  ASSERT_TRUE(write_success_1);
  ASSERT_TRUE(write_success_2);
  ASSERT_TRUE(write_success_3);

  std::string getBlock1;
  std::string getBlock2;
  std::string getBlock3;

  ASSERT_TRUE(filesystem.hasBlock(10));
  ASSERT_TRUE(filesystem.hasBlock(11));
  ASSERT_TRUE(filesystem.hasBlock(12));

  filesystem.getBlock(10, getBlock1);
  filesystem.getBlock(11, getBlock2);
  filesystem.getBlock(12, getBlock3);

  ASSERT_EQ(0, blk.compare(0, blk.length(), getBlock1));
  ASSERT_EQ(0, blk.compare(0, blk.length(), getBlock2));
  ASSERT_EQ(0, blk.compare(0, blk.length(), getBlock3));
}

TEST_F(NativeFSTest, CanRemoveBlock) {
  NativeFS filesystem(backing);
  ASSERT_TRUE(filesystem.rmBlock(1));
  ASSERT_FALSE(filesystem.hasBlock(1));
  ASSERT_TRUE(filesystem.rmBlock(2));
  ASSERT_FALSE(filesystem.hasBlock(2));

  ASSERT_TRUE(filesystem.rmBlock(10));
  ASSERT_TRUE(filesystem.rmBlock(11));
  ASSERT_TRUE(filesystem.rmBlock(12));
}

TEST_F(NativeFSTest, RemoveNonExistBlockReturnsError) {
  NativeFS filesystem(backing);
  ASSERT_FALSE(filesystem.rmBlock(3));
}

TEST_F(NativeFSTest, CanCoalesce) {
  NativeFS filesystem(backing);
  // Fill the disk with blocks of size 1/2 MAX BLOCK.
  {
    std::string halfblk(MAX_BLOCK_SIZE / 2, 'z');
    for (int i = 0; i < (DISK_SIZE - RESERVED_SIZE) / (MAX_BLOCK_SIZE / 2);
         i++) {
      ASSERT_TRUE(filesystem.writeBlock(i, halfblk));
    }
  }
  // Then free the blocks and attempt to add block of size MAX BLOCK.
  {
    std::string fullblk(MAX_BLOCK_SIZE, 'z');
    for (int i = 0; i < (DISK_SIZE - RESERVED_SIZE) / (MAX_BLOCK_SIZE / 2);
         i++) {
      ASSERT_TRUE(filesystem.rmBlock(i));
    }
    // Now write a few big blocks.
    for (int i = 0; i < (DISK_SIZE - RESERVED_SIZE) / MAX_BLOCK_SIZE; i++) {
      std::string readblk;
      ASSERT_TRUE(filesystem.writeBlock(i, fullblk));
      ASSERT_TRUE(filesystem.getBlock(i, readblk));
      // Make sure contents are the same.
      ASSERT_EQ(fullblk, readblk);
    }

    // Now remove the big blocks.
    for (int i = 0; i < (DISK_SIZE - RESERVED_SIZE) / MAX_BLOCK_SIZE; i++) {
      ASSERT_TRUE(filesystem.rmBlock(i));
    }
  }
}

TEST_F(NativeFSTest, CanAppend) {
  NativeFS filesystem(backing);
  {
    // Currently, no matter how big the block is sent to write, the fs
    // allocates MIN_BLOCK_SIZE. This test makes sure that even though
    // block memory might be wasted, reading is still correct.
    std::string  smallerThanMinBlk_a(MIN_BLOCK_SIZE / 2, 'a');
    ASSERT_TRUE(filesystem.writeBlock(0, smallerThanMinBlk_a));

    std::string readBlk;
    ASSERT_TRUE(filesystem.getBlock(0, readBlk));
    ASSERT_EQ(smallerThanMinBlk_a, readBlk);
    ASSERT_TRUE(filesystem.rmBlock(0));
  }
}

TEST_F(NativeFSTest, VerifySpaceWasted) {
  NativeFS filesystem(backing_smaller);
  {
    // This test verifies that space is purely wasted when the block sent
    // are smaller than MIN_BLOCK_SIZE.

    // Firstly, write mostly big blocks
    std::string fullblk(MAX_BLOCK_SIZE, 'z');
    int64 memory_left = DISK_SIZE - RESERVED_SIZE;
    int i = 0;
    for (; i < (DISK_SIZE - RESERVED_SIZE) / MAX_BLOCK_SIZE - 1; i++) {
      std::string readblk;
      ASSERT_TRUE(filesystem.writeBlock(i, fullblk));
      ASSERT_TRUE(filesystem.getBlock(i, readblk));
      // Make sure contents are the same.
      ASSERT_EQ(fullblk, readblk);
      memory_left -= MAX_BLOCK_SIZE;
    }

    // Secondly, write some semi-big blocks.
    size_t semi_big_block_size = 1 << ((nativefs::MIN_BLOCK_POWER
                                        + nativefs::MAX_BLOCK_POWER)/2);
    std::string semiblk(semi_big_block_size, 'z');
    int64 num_blocks = memory_left / semi_big_block_size - 1;
    int k;
    for (k = i; k < i + num_blocks; k++) {
      std::string readblk;
      ASSERT_TRUE(filesystem.writeBlock(k, semiblk));
      ASSERT_TRUE(filesystem.getBlock(k, readblk));
      // Make sure contents are the same.
      ASSERT_EQ(semiblk, readblk);
      memory_left -= semi_big_block_size;
    }

    // Even though block that is smaller than min is requested, MIN_BLOCK_SIZE
    // is allocated and used.
    std::string  smallerThanMinBlk_a(MIN_BLOCK_SIZE / 2, 'a');
    for (int j = k; j < k + memory_left / MIN_BLOCK_SIZE; j++) {
      std::string readblk;
      ASSERT_TRUE(filesystem.writeBlock(j, smallerThanMinBlk_a));
      ASSERT_TRUE(filesystem.getBlock(j, readblk));
      // Make sure contents are the same.
      ASSERT_EQ(smallerThanMinBlk_a, readblk);
    }
    ASSERT_FALSE(filesystem.writeBlock(
      (DISK_SIZE - RESERVED_SIZE) / MIN_BLOCK_SIZE,
                                       smallerThanMinBlk_a));
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::InitGoogleMock(&argc, argv);
  system("truncate -s 1000000000 _NATIVEFS_TEST_FS");
  system("truncate -s 1000000000 _NATIVEFS_TEST_FS2");
  int result = RUN_ALL_TESTS();
  system("rm -f _NATIVEFS_TEST_FS");
  return result;
}
