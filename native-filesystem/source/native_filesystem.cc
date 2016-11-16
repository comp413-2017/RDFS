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
}

namespace nativefs {
	const std::string NativeFS::CLASS_NAME = ": **NativeFS** : ";

	NativeFS::NativeFS(NativeFS& other) :
		freeLists(other.freeLists),
		blocks(other.blocks),
		disk_out(std::move(other.disk_out)),
		disk_in(std::move(other.disk_in)) {}

	NativeFS::NativeFS(std::string fname) : disk_in(fname, std::ios::binary | std::ios::in), disk_out(fname, std::ios::binary | std::ios::out) {
		// If the magic bytes exist, then load block info from the drive.
		std::string magic(MAGIC.size(), 0);
		blocks = new block_info[BLOCK_LIST_LEN];
		disk_in.seekg(0);
		disk_in.read(&magic[0], magic.size());
		if (magic == MAGIC) {
			LOG(INFO) << "Reloading existing block list...";
			disk_in.read((char *) &blocks[0], BLOCK_LIST_SIZE);
		} else {
			LOG(INFO) << "No block list found, constructing from scratch...";
			std::for_each(&blocks[0], &blocks[BLOCK_LIST_LEN], resetBlock);
			flushBlocks();
		}
        freeLists.resize(FREE_LIST_SIZE);
		std::sort(&blocks[0], &blocks[BLOCK_LIST_LEN],
				[](const block_info& a, const block_info& b) -> bool {
					return a.offset < b.offset;
				});

		// Add free space before the first block.
		freeRange(0, blocks[0].offset);
		// Add free space between blocks.
		for (int i = 0; i < BLOCK_LIST_LEN - 1; i++) {
			if (blocks[i].offset + blocks[i].len == blocks[i+1].offset) {
				continue;
			}
			freeRange(blocks[i].offset + blocks[i].len, blocks[i+1].offset);
		}
		// Add free space between the last block and the end of disk.
		freeRange(blocks[BLOCK_LIST_LEN - 1].offset + blocks[BLOCK_LIST_LEN - 1].len, DISK_SIZE);
		// printFreeBlocks();
	}

	NativeFS::~NativeFS() {
		flushBlocks();
		delete[] blocks;
	}

	void NativeFS::flushBlocks() {
		LOG(INFO) << "Flushing blocks to storage.";
		disk_out.seekp(0);
		disk_out.write(&MAGIC[0], MAGIC.size());
		disk_out.write((const char*) &blocks[0], BLOCK_LIST_SIZE);
		disk_out.flush();
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
		for (int i = 0; i < freeLists.size(); i++) {
			std::cout << "BLOCKS " << i << ": ";
			for (auto offset: freeLists[i]) {
				std::cout << offset << ",";
			}
			std::cout << std::endl;
		}
	}

	void NativeFS::printKnownBlocks() {
        LOG(INFO) << "Known blocks:";
		for (int i = 0; i < BLOCK_LIST_LEN; i++) {
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
			LOG(ERROR) << "Failed attempting to allocated block of power " << ceiling;
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
	bool NativeFS::writeBlock(uint64_t id, std::string blk) {
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
		disk_out.seekp(offset);
		disk_out << blk;
		disk_out.flush();

		block_info info;
		info.blockid = id;
		info.offset = offset;
		info.len = len;

		std::lock_guard<std::mutex> lock(listMtx);
		switch (addBlock(info)) {
			case 0:
				flushBlocks();
				break;
			case 1:
				LOG(ERROR) << "Could not find space for block " << info.blockid << " (shouldn't happen!)";
				return false;
			case 2:
				LOG(ERROR) << "Block wih id " << info.blockid << " already exists on this DataNode";
				return false;
		}

		return true;

	}

	/**
	 * Returns 0 on success, 1 if no space, 2 if already exists
	 */
	int NativeFS::addBlock(const block_info& info) {
		// Make sure this block doesn't already exist on this datanode
		for (int i = 0; i < BLOCK_LIST_LEN; i++) {
			if (blocks[i].blockid == info.blockid) {
				return 2;
			}
		}
		// Instead block_info into array
		for (int i = 0; i < BLOCK_LIST_LEN; i++) {
			if (blocks[i].len == 0) {
				blocks[i] = info;
				return 0;
			}
		}
		return 1;
	}

	/**
	* Given an ID, returns a block buffer
	**/
	std::string NativeFS::getBlock(uint64_t id, bool& success) {
		// Look in map and get filename
		block_info info;
		{
			std::lock_guard<std::mutex> lock(listMtx);
			// Look up the block info for this id.
			bool found = false;
			for (int i = 0; i < BLOCK_LIST_LEN; i++) {
				if (blocks[i].blockid == id) {
					info = blocks[i];
					found = true;
					break;
				}
			}
			if (!found) {
				success = false;
				return "";
			}
		}
		LOG(INFO) << "Reading block " << id << " length=" << info.len << " at offset=" << info.offset;
		std::string data(info.len, 0);
		disk_in.seekg(info.offset);
		disk_in.read(&data[0], info.len);
		success = true;
		return data;
	}

	/**
	* Given an ID, deletes a block. Returns false on id not found, true otherwise
	**/
	bool NativeFS::rmBlock(uint64_t id) {
		std::lock_guard<std::mutex> lock(listMtx);
		for (int i = 0; i < BLOCK_LIST_LEN; i++) {
			if (blocks[i].blockid == id) {
				uint64_t offset = blocks[i].offset;
				uint32_t len = blocks[i].len;
				// Make sure the interval provided is a power of two.
				freeRange(offset, offset + std::pow(2, powerup(len)));
				resetBlock(blocks[i]);
				flushBlocks();
				return true;
			}
		}
		return false;

	}

	long NativeFS::getTotalSpace() {
		return DISK_SIZE;
	}

	long NativeFS::getFreeSpace() {
		long allocatedSize = 0;
		for (int i = 0; i < BLOCK_LIST_LEN; i++) {
			allocatedSize += blocks[i].len;
		}
		return getTotalSpace() - allocatedSize;
	}
}
