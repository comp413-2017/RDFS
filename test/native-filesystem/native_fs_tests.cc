// Copyright 2017 Rice University, COMP 413 2017

#include "native_filesystem.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <easylogging++.h>
#include <string.h>
INITIALIZE_EASYLOGGINGPP
using ::testing::AtLeast;

using nativefs::NativeFS;
using nativefs::MAX_BLOCK_SIZE;
using nativefs::DISK_SIZE;
using nativefs::RESERVED_SIZE;

class NativeFSTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    blk = "here's some data to write in the block";
    backing = "_NATIVEFS_TEST_FS";
  }
  std::string blk;
  std::string backing;
};

// TODO(2016): Test writing and getting multiple blocks

TEST_F(NativeFSTest, CanWriteBlock) {
  NativeFS filesystem(backing);
  ASSERT_EQ(true, filesystem.writeBlock(1, blk));
}

TEST_F(NativeFSTest, CanWriteAndGetBlock) {
  NativeFS filesystem(backing);
  bool write_success = filesystem.writeBlock(2, blk);
  ASSERT_EQ(true, write_success);
  std::string newBlock;
  ASSERT_EQ(true, filesystem.hasBlock(2));
  bool success = filesystem.getBlock(2, newBlock);
  ASSERT_EQ(true, success);
  ASSERT_EQ(0, blk.compare(0, blk.length(), newBlock));
}

TEST_F(NativeFSTest, CanRemoveBlock) {
  NativeFS filesystem(backing);
  ASSERT_EQ(true, filesystem.rmBlock(1));
  ASSERT_EQ(false, filesystem.hasBlock(1));
  ASSERT_EQ(true, filesystem.rmBlock(2));
  ASSERT_EQ(false, filesystem.hasBlock(2));
}

TEST_F(NativeFSTest, RemoveNonExistBlockReturnsError) {
  NativeFS filesystem(backing);
  ASSERT_EQ(false, filesystem.rmBlock(3));
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
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::InitGoogleMock(&argc, argv);
  system("truncate -s 1000000000 _NATIVEFS_TEST_FS");
  int result = RUN_ALL_TESTS();
  system("rm -f _NATIVEFS_TEST_FS");
  return result;
}
