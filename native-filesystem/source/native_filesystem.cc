#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <map>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <easylogging++.h>
#include <mutex>
#include "native_filesystem.h"

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
	return (t > val)?p - 1:p;
}

// Reset the fields of blk to default empty state.
static void resetBlock(nativefs::block_info& blk) {
	blk.blockid = 0;
	blk.offset = nativefs::DISK_SIZE;
	blk.len = 0;
	blk.free = true;
}

namespace nativefs {
	const std::string NativeFS::CLASS_NAME = ": **NativeFS** : ";

	NativeFS::NativeFS(std::string fname) : disk(fname, std::ios::binary | std::ios::in | std::ios::out) {
		// If the magic bytes exist, then load block info from the drive.
		std::string magic(MAGIC.size(), 'b');
		blocks = new block_info[BLOCK_LIST_LEN];
		disk.seekg(0);
		disk.read(&magic[0], magic.size());
		if (magic == MAGIC) {
			LOG(INFO) << CLASS_NAME << "Reloading existing block list...";
			disk.read((char *) &blocks[0], BLOCK_LIST_SIZE);
		} else {
			LOG(INFO) << CLASS_NAME << "No block list found, constructing from scratch...";
			std::for_each(&blocks[0], &blocks[BLOCK_LIST_LEN], resetBlock);
			flushBlocks();
		}
		constructFreeLists();
	}

	NativeFS::~NativeFS() {
		flushBlocks();
		delete[] blocks;
	}

	void NativeFS::flushBlocks() {
		LOG(INFO) << CLASS_NAME << "Flushing blocks to storage.";
		disk.seekp(0);
		disk.write(&MAGIC[0], MAGIC.size());
		disk.write((const char*) &blocks[0], BLOCK_LIST_SIZE);
		disk.flush();
	}

	void NativeFS::constructFreeLists() {
		freeLists.clear();
		freeLists.resize(FREE_LIST_SIZE);
		// Sort blocks by order of offset on disk.
		std::sort(&blocks[0], &blocks[BLOCK_LIST_LEN],
				[](const block_info& a, const block_info& b) -> bool {
					return a.offset < b.offset;
				});

		// Add free space before the first block.
		freeRange(RESERVED_SIZE, blocks[0].offset);
		// Add free space between blocks.
		for (size_t i = 0; i < BLOCK_LIST_LEN - 1; i++) {
			if (blocks[i].offset + blocks[i].len == blocks[i+1].offset) {
				continue;
			}
			freeRange(blocks[i].offset + blocks[i].len, blocks[i+1].offset);
		}
		// Add free space between the last block and the end of disk.
		freeRange(blocks[BLOCK_LIST_LEN - 1].offset + blocks[BLOCK_LIST_LEN - 1].len, DISK_SIZE);
	}

	void NativeFS::freeRange(uint64_t start, uint64_t end) {
		// Sanity check: make sure start <= end <= DISK_SIZE.
		end = std::max(start, std::min(end, DISK_SIZE));
		// Fill in with the largest blocks possible until no more fit.

		// If the block is larger than max block size, then split it up into blocks of max size.
		if (end - start > MAX_BLOCK_SIZE) {
			size_t i = start;
			for (; i + MAX_BLOCK_SIZE < end; i+= MAX_BLOCK_SIZE) {
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
        LOG(INFO) << "Free blocks:";
		for (size_t i = 0; i < freeLists.size(); i++) {
			std::cout << "BLOCKS " << i << ": ";
			for (auto offset: freeLists[i]) {
				std::cout << offset << ",";
			}
			std::cout << std::endl;
		}
	}

	void NativeFS::printKnownBlocks() {
        LOG(INFO) << "Known blocks:";
		for (size_t i = 0; i < BLOCK_LIST_LEN; i++) {
			if (blocks[i].len != 0) {
				auto info = blocks[i];
				LOG(INFO) << "Found block: " << info.blockid << " at " << info.offset << " with len " << info.len;
			}
		}
	}

	bool NativeFS::allocateBlock(size_t size, uint64_t& offset) {
		// We cannot allocate a block smaller than MIN_BLOCK_SIZE.
		size = std::max(MIN_BLOCK_SIZE, size);
		size_t ceiling = powerup(size);
		if (ceiling > MAX_BLOCK_POWER) {
			LOG(ERROR) << CLASS_NAME << "Failed attempting to allocated block of power " << ceiling;
			return false;
		}
		auto& freeBlocks = freeLists[ceiling - MIN_BLOCK_POWER];
		if (freeBlocks.empty()) {
			bool success =  allocateBlock(size * 2, offset);
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

	/**
	* Given an ID, write the given block to the native filesystem. Returns true/false on success/failure.
	**/
	bool NativeFS::writeBlock(uint64_t id, const std::string& blk) {
		size_t len = blk.size();
		uint64_t offset;
		{
			std::lock_guard<std::mutex> lock(listMtx);
			if (!allocateBlock(len, offset)) {
				LOG(ERROR) << CLASS_NAME << "Could not find a free block to fit " << len;
			    return false;
			}
		}
		LOG(INFO) << CLASS_NAME << "Writing block " << id << " to offset " << offset;
		disk.seekp(offset);
		disk << blk;
		disk.flush();

		block_info info;
		info.blockid = id;
		info.offset = offset;
		info.len = len;
		info.free = false;

		std::lock_guard<std::mutex> lock(listMtx);
		switch (addBlock(info)) {
			case 0:
				flushBlocks();
				break;
			case 1:
				LOG(ERROR) << CLASS_NAME << "Could not find space for block " << info.blockid << " (shouldn't happen!)";
				return false;
			case 2:
				LOG(ERROR) << CLASS_NAME << "Block wih id " << info.blockid << " already exists on this DataNode";
				return false;
		}

		return true;

	}

	/**
	 * Returns 0 on success, 1 if no space, 2 if already exists
	 */
	int NativeFS::addBlock(const block_info& info) {
		// Make sure this block doesn't already exist on this datanode
		for (size_t i = 0; i < BLOCK_LIST_LEN; i++) {
			if (blocks[i].blockid == info.blockid && !blocks[i].free) {
				return 2;
			}
		}
		// Instead block_info into array
		for (size_t i = 0; i < BLOCK_LIST_LEN; i++) {
			if (blocks[i].len == 0) {
				blocks[i] = info;
				return 0;
			}
		}
		return 1;
	}

	/**
	* Read the contents of given block id.
	**/
	bool NativeFS::getBlock(uint64_t id, std::string& blk) {
		// Look in map and get filename
		block_info info;
		{
			std::lock_guard<std::mutex> lock(listMtx);
			// Look up the block info for this id.
			bool found = false;
			for (size_t i = 0; i < BLOCK_LIST_LEN; i++) {
				if (blocks[i].blockid == id && !blocks[i].free) {
					info = blocks[i];
					found = true;
					break;
				}
			}
			if (!found) {
				return false;
			}
		}
		LOG(INFO) << CLASS_NAME << "Reading block " << id << " length=" << info.len << " at offset=" << info.offset;
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
				flushBlocks();
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
}
