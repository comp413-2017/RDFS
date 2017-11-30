// Copyright 2017 Rice University, COMP 413 2017

#include "native_filesystem.h"

#include <easylogging++.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <mutex>
#include <native_filesystem.h>

// Return the power of two >= val.
static size_t powerup(uint64_t val) {
  uint64_t t = 1;
  size_t p = 0;
  while (t < val) {
    t *= 2;
    p++;
  }
  return p;
}

// Return the power of two <= val.
static size_t powerdown(uint64_t val) {
  uint64_t t = 1;
  size_t p = 0;
  while (t < val) {
    t *= 2;
    p++;
  }
  // Only subtract 1 from p if we exceeded val.
  return (t > val) ? p - 1 : p;
}

// Reset the fields of blk to default empty state.
static void resetBlock(nativefs::block_info &blk) {
  blk.blockid = 0;
  blk.offset = nativefs::DISK_SIZE;
  blk.len = 0;
  blk.free = true;
}

namespace nativefs {

NativeFS::NativeFS(std::string fname) :
    disk(fname, std::ios::binary | std::ios::in | std::ios::out) {
  const std::string magic = MAGIC;

  // If the magic bytes exist, then load block info from the drive.
  std::string maybe_magic(magic.size(), 'b');
  blocks = new block_info[BLOCK_LIST_LEN];
  disk.seekg(0);
  disk.read(&maybe_magic[0], maybe_magic.size());
  if (maybe_magic == magic) {
    LOG(INFO) << "Reloading existing block list...";
    disk.read(reinterpret_cast<char *>(&blocks[0]), BLOCK_LIST_SIZE);
  } else {
    LOG(INFO) << "No block list found, constructing from scratch...";
    std::for_each(&blocks[0], &blocks[BLOCK_LIST_LEN], resetBlock);
    flushAllBlocks();
  }
  constructFreeLists();
}

void NativeFS::constructFreeLists() {
  freeLists.clear();
  freeLists.resize(FREE_LIST_SIZE);
  // Sort blocks by order of offset on disk.
  std::sort(&blocks[0], &blocks[BLOCK_LIST_LEN],
            [](const block_info &a, const block_info &b) -> bool {
              return a.offset < b.offset;
            });

  // Add free space before the first block.
  freeRange(RESERVED_SIZE, blocks[0].offset);
  // Add free space between blocks.
  for (size_t i = 0; i < BLOCK_LIST_LEN - 1; i++) {
    if (blocks[i].offset + blocks[i].allocated_size == blocks[i + 1].offset) {
      continue;
    }
    // Do not free the range
    if (blocks[i].allocated_size <= MIN_BLOCK_SIZE) {
      continue;
    }
    freeRange(blocks[i].offset + blocks[i].allocated_size, blocks[i + 1].offset);
  }
  // Add free space between the last block and the end of disk.
  freeRange(blocks[BLOCK_LIST_LEN - 1].offset + blocks[BLOCK_LIST_LEN - 1].allocated_size,
            DISK_SIZE);
}

NativeFS::~NativeFS() {
  flushAllBlocks();
  delete[] blocks;
}

void NativeFS::flushAllBlocks() {
  LOG(INFO) << "Flushing blocks to storage.";
  disk.seekp(0);
  disk.write(&MAGIC[0], strlen(MAGIC));
  disk.write((const char *) &blocks[0], BLOCK_LIST_SIZE);
  disk.flush();
}

void NativeFS::flushBlock(int block_index) {
  LOG(INFO) << "Flushing block " << block_index << " to storage.";
  disk.seekp(strlen(MAGIC) + block_index * sizeof(block_info));
  disk.write((const char *) &blocks[block_index], sizeof(block_info));
  disk.flush();
}

void NativeFS::freeRange(uint64_t start, uint64_t end) {
  // Sanity check: make sure start <= end <= DISK_SIZE.
  end = std::max(start, std::min(end, DISK_SIZE));
  // Fill in with the largest blocks possible until no more fit.

  // If the block is larger than max block size, then split it up into blocks of
  // max size.
  if (end - start > MAX_BLOCK_SIZE) {
    size_t i = start;
    for (; i + MAX_BLOCK_SIZE < end; i += MAX_BLOCK_SIZE) {
      freeRange(i, i + MAX_BLOCK_SIZE);
    }
    freeRange(i, end);
    // Otherwise, recursively divide into the largest possible blocks
  } else {
    while ((end - start) >= MIN_BLOCK_SIZE) {
      size_t fit = powerdown(end - start);
      size_t idx = fit - MIN_BLOCK_POWER;
      uint64_t offset = start;
      freeLists[idx].push_back(offset);
      start += std::pow(2, fit);
    }
  }
}

void NativeFS::printFreeBlocks() {
  LOG(DEBUG) << "Free blocks:";
  for (size_t i = 0; i < freeLists.size(); i++) {
    LOG(DEBUG) << "BLOCKS " << i << ": ";
    for (auto offset : freeLists[i]) {
      LOG(DEBUG) << offset << ",";
    }
  }
}

std::vector<std::uint64_t> NativeFS::getKnownBlocks() {
  std::vector<std::uint64_t> vector;
  // LOG(INFO) << "Known blocks:";
  for (size_t i = 0; i < BLOCK_LIST_LEN; i++) {
    if (blocks[i].len != 0) {
      auto info = blocks[i];
      vector.push_back(info.blockid);
    }
  }
  return vector;
}

bool NativeFS::allocateBlock(size_t size, uint64_t &offset) {
  // We cannot allocate a block smaller than MIN_BLOCK_SIZE.
  size = std::max(MIN_BLOCK_SIZE, size);
  size_t ceiling = powerup(size);
  if (ceiling > MAX_BLOCK_POWER) {
    LOG(ERROR) << "Failed attempting to allocated block of power " << ceiling;
    return false;
  }
  auto &freeBlocks = freeLists[ceiling - MIN_BLOCK_POWER];
  if (freeBlocks.empty()) {
    bool success = allocateBlock(size * 2, offset);
    // If successful, split the allocated block in half.
    if (success) {
      freeBlocks.push_back(offset + (std::pow(2, ceiling)));
    }
    return success;
  } else {
    offset = freeBlocks[freeBlocks.size() - 1];
    freeBlocks.pop_back();
    return true;
  }
}

size_t NativeFS::findBlock(uint64_t block_id) {
  for (size_t i = 0; i < BLOCK_LIST_LEN; i++) {
    if (blocks[i].blockid == block_id && !blocks[i].free) {
      return i;
    }
  }
  return UINT64_MAX;
}

/**
* Given an ID, write the given block to the native filesystem. Returns true/false on success/failure.
**/
bool NativeFS::writeBlock(uint64_t id, const std::string &blk) {
  size_t len = blk.size();
  size_t i;
  if ((i = findBlock(id)) != UINT64_MAX) {
    block_info info = blocks[i];
    if (len + info.len > info.allocated_size) {
      LOG(ERROR) << "Trying to write more than block " << id << " has allocated";
      return false;
    }

    uint64_t offset = info.offset + info.len;
    LOG(INFO) << "Writing block " << id << " to offset " << offset;
    disk.seekp(offset);
    disk << blk;
    disk.flush();

    // Updates block info
    blocks[i].len += len;

    flushBlock(i);
    return true;
  }
  uint64_t offset;
  {
    std::lock_guard<std::mutex> lock(listMtx);
    if (!allocateBlock(len, offset)) {
      LOG(ERROR) << "Could not find a free block to fit " << len;
      return false;
    }
  }
  LOG(INFO) << "Writing block " << id << " to offset " << offset;
  disk.seekp(offset);
  disk << blk;
  disk.flush();

  block_info info;
  info.blockid = id;
  info.offset = offset;
  info.len = len;
  info.free = false;
  info.allocated_size = std::max(MIN_BLOCK_SIZE, len);

  std::lock_guard<std::mutex> lock(listMtx);
  int added_index = addBlock(info);
  switch (added_index) {
    case -1:
      LOG(ERROR) << "Block wih id "
                 << info.blockid
                 << " already exists on this DataNode";
      return false;
    case -2:
      // This case shouldn't happen
      LOG(ERROR) << "Could not find space for block " << info.blockid;
      return false;
    default: flushBlock(added_index);
  }

  return true;
}

/**
 * Returns the index added on success. If already exists, return -1. If no
 * space, return -2.
 */
int NativeFS::addBlock(const block_info &info) {
  // Make sure this block doesn't already exist on this datanode
  for (size_t i = 0; i < BLOCK_LIST_LEN; i++) {
    if (blocks[i].blockid == info.blockid && !blocks[i].free) {
      return -1;
    }
  }
  // Insert block_info into array
  for (size_t i = 0; i < BLOCK_LIST_LEN; i++) {
    if (blocks[i].len == 0 && blocks[i].free == true) {
      blocks[i] = info;
      return i;
    }
  }
  return -2;
}

/**
 * Fetch block_info for an id. Assumes it has a lock on the block list.
 */
bool NativeFS::fetchBlock(uint64_t id, block_info &info) {
  for (size_t i = 0; i < BLOCK_LIST_LEN; i++) {
    if (blocks[i].blockid == id && !blocks[i].free) {
      info = blocks[i];
      return true;
    }
  }
  return false;
}

/**
 * Check existence of block with given id.
 */
bool NativeFS::hasBlock(uint64_t id) {
  block_info info;
  std::lock_guard<std::mutex> lock(listMtx);
  return fetchBlock(id, info);
}

/**
* Read the contents of given block id.
*/
bool NativeFS::getBlock(uint64_t id, std::string &blk) {
  // Look in map and get filename
  block_info info;
  {
    std::lock_guard<std::mutex> lock(listMtx);
    if (!fetchBlock(id, info)) {
      return false;
    }
  }
  LOG(INFO) << "Reading block "
            << id << " length=" << info.len << " at offset=" << info.offset;
  blk.resize(info.len);
  disk.seekg(info.offset);
  disk.read(&blk[0], info.len);
  return true;
}

/**
* Given an ID, deletes a block. Returns false on id not found, true otherwise
**/
bool NativeFS::rmBlock(uint64_t id) {
  std::lock_guard<std::mutex> lock(listMtx);
  for (size_t i = 0; i < BLOCK_LIST_LEN; i++) {
    if (blocks[i].blockid == id && !blocks[i].free) {
      uint64_t offset = blocks[i].offset;
      uint32_t len = blocks[i].len;
      resetBlock(blocks[i]);
      // Coalesce by reconstructing the free lists.
      constructFreeLists();
      flushBlock(i);
      return true;
    }
  }
  return false;
}

uint64_t NativeFS::getTotalSpace() {
  return DISK_SIZE;
}

uint64_t NativeFS::getFreeSpace() {
  uint64_t allocatedSize = 0;
  for (size_t i = 0; i < BLOCK_LIST_LEN; i++) {
    allocatedSize += blocks[i].len;
  }
  return getTotalSpace() - allocatedSize;
}

bool NativeFS::extendBlock(uint64_t block_id, std::string block_data) {
  block_info blockInfo;
  size_t block_index;
  if ((block_index = findBlock(block_id)) == UINT64_MAX) {
    LOG(ERROR) << "[native_filesystem] Failed to find block for "
               << block_id;
    return false;
  }
  blockInfo = blocks[block_index];
  uint64_t new_len = blockInfo.len + block_data.length();
  uint64_t new_offset;
  {
    std::lock_guard<std::mutex> lock(listMtx);
    if (!allocateBlock(new_len, new_offset)) {
      LOG(ERROR) << "Could not find a free block to fit " << new_len;
      return false;
    }
  }
  LOG(INFO) << "Writing block " << block_id << " to offset " << new_offset;
  // Copy the old block over
  char * buffer = new char[new_len];
  disk.read(buffer, blockInfo.len);
  // Writes the old block
  disk.seekp(new_offset);
  disk.write(buffer, blockInfo.len);
  disk << block_data;

  {
    std::lock_guard<std::mutex> lock(listMtx);
    // Free the old block
    freeRange(blockInfo.offset, blockInfo.len);

    // Set the new block info
    blocks[block_index].allocated_size = new_len;
    blocks[block_index].len = new_len;
    blocks[block_index].offset = new_offset;
  }

  return true;
}
}  // namespace nativefs
