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
#include "block_queue.h"


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
		disk_in.seekg(0);
		disk_in.read(&magic[0], magic.size());
		if (magic == MAGIC) {
			LOG(INFO) << "Reloading existing block list...";
			disk_in.read((char *) &blocks[0], BLOCK_LIST_SIZE);
		} else {
			LOG(INFO) << "No block list found, constructing from scratch...";
			std::for_each(blocks.begin(), blocks.end(), resetBlock);
			flushBlocks();
		}
		std::sort(blocks.begin(), blocks.end(),
				[](const block_info& a, const block_info& b) -> bool {
					return a.offset < b.offset;
				});
		std::cout << "freeing ranges." << std::endl;
		// Add free space before the first block.
		freeRange(0, blocks[0].offset);
		// Add free space between blocks.
		for (int i = 0; i < blocks.size() - 1; i++) {
			std::cout << "freeing " << i << std::endl;
			freeRange(blocks[i].offset, blocks[i+1].offset);
		}
		std::cout << "freeing last one" << std::endl;
		// Add free space between the last block and the end of disk.
		freeRange(blocks[blocks.size() - 1].offset, DISK_SIZE);
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
		while ((end - start) >= MIN_BLOCK_SIZE) {
			size_t fit = powerdown(end - start);
			size_t idx = fit - MIN_BLOCK_POWER;
			auto blk = std::make_shared<free_block>();
			blk->offset = start;
			blk->next = freeLists[idx];
			freeLists[idx] = blk;
			start += std::pow(2, fit);
		}
	}

	bool NativeFS::allocateBlock(size_t size, uint64_t& offset) {
		// TODO: do this one
		return false;
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

		listMtx.lock();
		if (!addBlock(info)) {
			LOG(ERROR) << "Could not find space for block " << info.blockid << " (shouldn't happen!)";
		} else {
			flushBlocks();
		}
		listMtx.unlock();

		return true;

	}

	bool NativeFS::addBlock(const block_info& info) {
		for (int i = 0; i < BLOCK_LIST_LEN; i++) {
			if (blocks[i].len == 0) {
				blocks[i] = info;
				return true;
			}
		}
		return false;
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
			for (int i = 0; i < blocks.size(); i++) {
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
		for (int i = 0; i < blocks.size(); i++) {
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

	long getTotalSpace() {
		return DISK_SIZE;
	}

	long getFreeSpace() {
		long allocatedSize = 0;
		for (int i = 0; i < blocks.size(); i++) {
			allocatedSize += blocks[i].len;
		}
		return getTotalSpace() - allocatedSize;
	}
}
