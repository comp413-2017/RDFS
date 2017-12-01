// Copyright 2017 Rice University, COMP 413 2017

#include <easylogging++.h>
#include <stdio.h>
#include <stdlib.h>

#include <fstream>
#include <mutex>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#pragma once

namespace nativefs {

typedef struct {
  uint64_t blockid;
  uint64_t offset;  // Address in the FS
  uint32_t len; // length of the data
  // in the block
  uint32_t allocated_size; // total
  // allocated size for this block
  bool free;
} block_info;

const size_t MIN_BLOCK_POWER = 13;
const size_t MAX_BLOCK_POWER = 27;
constexpr size_t MIN_BLOCK_SIZE = 1 << MIN_BLOCK_POWER;
constexpr size_t MAX_BLOCK_SIZE = 1 << MAX_BLOCK_POWER;
constexpr size_t FREE_LIST_SIZE = MAX_BLOCK_POWER - MIN_BLOCK_POWER + 1;
constexpr size_t DISK_SIZE = MAX_BLOCK_SIZE * 6;
constexpr size_t BLOCK_LIST_LEN = DISK_SIZE / MIN_BLOCK_SIZE;
constexpr size_t BLOCK_LIST_SIZE = BLOCK_LIST_LEN * sizeof(block_info);
// If MAGIC changes, make sure to change the 8 in RESERVED_SIZE too.
const char MAGIC[] = "DEADBEEF";
// Reserved space for magic bytes + block_info array.
constexpr size_t RESERVED_SIZE = BLOCK_LIST_SIZE + 8;

class NativeFS {
 public:
  /**
   * Construct a NativeFS backed by the given filename.
   */
  explicit NativeFS(std::string);
  /**
   * Destroy NativeFS, free block array.
   */
  ~NativeFS();
  /**
   * Write provided block contents to a block with given id. On success,
   * return true. Otherwise false (e.g. no free space).
   */
  bool writeBlock(uint64_t, const std::string &);
  /**
   * Get the contents of the block with given id. If successful, return
   * true and set string reference to retreived contents. Otherwise
   * return false.
   */
  bool getBlock(uint64_t, std::string &);
  /**
   * Return whether this instance of NativeFS knows about a block with
   * given id.
   */
  bool hasBlock(uint64_t);
  /**
   * Delete contents of provided block id from this datanode. Return true
   * if delete successful, false otherwise (block id not found).
   */
  bool rmBlock(uint64_t);
  /**
   * Return the total capacity of this datanode.
   */
  uint64_t getTotalSpace();
  /**
   * Return an estimate of the remaining free space on this datanode.
   */
  uint64_t getFreeSpace();

  /**
   * For debugging, print the free block lists.
   */
  std::vector<std::uint64_t> getKnownBlocks();

  /**
 * Attempt to fetch block info for block of given id, write to info
 * reference. Return whether it exists.
 */
  bool fetchBlock(uint64_t, block_info &info);

  /**
   * Extends the current block and append the block data.
   */
  bool extendBlock(uint64_t block_id, std::string block_data);

 private:
  /**
   * Attempt to place provided block info in the block list. Returns
   * 0 on success, 1 if no space, 2 if already exists
   */
  int addBlock(const block_info &info);
  /**
   * Mark the area of disk from start to end as free.
   */
  void freeRange(uint64_t start, uint64_t end);
  /**
   * Allocate space to fit provided size, write position to offset.
   * Return true if space was found, otherwise false.
   */
  bool allocateBlock(size_t size, uint64_t &offset);
  /**
   * Build the free ranges from allocated blocks.
   */
  void constructFreeLists();
  /**
   * Persist current block metadata to storage.
   */
  void flushAllBlocks();
  /**
   * Persist one particular block to storage.
   */
  void flushBlock(int block_index);

  size_t findBlock(uint64_t block_id);

  /**
   * For debugging, print the free blocks.
   */
  void printFreeBlocks();

  block_info *blocks;
  mutable std::mutex listMtx;
  std::vector<std::vector<uint64_t>> freeLists;
  std::fstream disk;
  static const std::string CLASS_NAME;
};

}  // namespace nativefs
