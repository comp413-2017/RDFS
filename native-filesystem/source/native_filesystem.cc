#include <iostream>
#include <fstream>
#include <sstream>
#include <map>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <easylogging++.h>
#include <mutex>
#include "native_filesystem.h"
#include "block_queue.h"


// Return the power of two >= val.
static uint64_t powerup(uint64_t val) {
	uint64_t t = 1;
	while (t < val) {
		t *= 2;
	}
	return t;
}

static void zeroBlock(nativefs::block_info& blk) {
	blk.blockid = 0;
	blk.offset = 0;
	blk.len = 0;
}

namespace nativefs {
	const std::string NativeFS::CLASS_NAME = ": **NativeFS** : ";

	NativeFS::NativeFS(NativeFS& other) :
		free64(other.free64),
		free128(other.free128),
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
			for (int i = 0; i < blocks.size(); i++) {
				zeroBlock(blocks[i]);
			}
			flushBlocks();
		}
		// Reconstruct free block list.
		for (uint64_t offset = RESERVED_SIZE; offset < DISK_SIZE; offset += MAX_BLOCK_SIZE) {
			auto block = std::make_shared<free_block>();
			block->offset = offset;
			block->next = free128;
			free128 = block;
		}
	}

	void NativeFS::flushBlocks() {
		LOG(INFO) << "Flushing blocks to storage.";
		disk_out.seekp(0);
		disk_out.write(&MAGIC[0], MAGIC.size());
		disk_out.write((const char*) &blocks[0], BLOCK_LIST_SIZE);
		disk_out.flush();
	}

	// Attempt to allocate a 128 megabyte block and write to offset. On
	// success, return true. On failure (no blocks available), return false.
	bool NativeFS::allocate128(uint64_t& offset) {
		if (free128 == nullptr) {
			return false;
		}
		offset = free128->offset;
		free128 = free128->next;
		return true;
	}

	// Attempt to allocate a 64 megabyte block and write to offset. On
	// success, return true. On failure (no blocks available), return false.
	bool NativeFS::allocate64(uint64_t& offset) {
		if (free64 == nullptr) {
			if (free128 == nullptr) {
				return false;
			}
			// Then free64 is empty but free128 has something, so split one
			// 128m block.
			allocate128(offset);
			uint64_t new_free_offset = offset + MAX_BLOCK_SIZE / 2;
			auto new_free_block = std::make_shared<free_block>();
			new_free_block->offset = new_free_offset;
			new_free_block->next = free64;
			free64 = new_free_block;
		} else {
			offset = free64->offset;
			free64 = free64->next;
		}
		return true;
	}

	/**
	* Given an ID, write the given block to the native filesystem. Returns true/false on success/failure.
	**/
	bool NativeFS::writeBlock(uint64_t id, std::string blk)
	{
		size_t len = blk.size();
		uint64_t offset;
		{
			std::lock_guard<std::mutex> lock(listMtx);
			if (len <= MAX_BLOCK_SIZE / 2) {
				// Then try to take a free 64 megabyte block, otherwise split a 128m block.
				if (!allocate64(offset)) {
					LOG(ERROR) << CLASS_NAME << "Could not find a free 64-megabyte block.";
					return false;
				}
			} else {
				if (!allocate128(offset)) {
					LOG(ERROR) << CLASS_NAME << "Could not find a free 128-megabyte block.";
					return false;
				}
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
	std::string NativeFS::getBlock(uint64_t id, bool& success)
	{
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
	bool NativeFS::rmBlock(uint64_t id)
	{
		std::lock_guard<std::mutex> lock(listMtx);
		for (int i = 0; i < blocks.size(); i++) {
			if (blocks[i].blockid == id) {
				uint64_t offset = blocks[i].offset;
				uint32_t len = blocks[i].len;
				zeroBlock(blocks[i]);
				auto new_free_block = std::make_shared<free_block>();
				new_free_block->offset = offset;
				if (len > MAX_BLOCK_SIZE / 2) {
					new_free_block->next = free128;
					free128 = new_free_block;
				} else {
					new_free_block->next = free64;
					free64 = new_free_block;
				}
				flushBlocks();
				return true;
			}
		}
		return false;

	}
}
