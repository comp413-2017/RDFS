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

namespace nativefs {
	const std::string NativeFS::CLASS_NAME = ": **NativeFS** : ";

	NativeFS::NativeFS(std::string fname) : disk_in(fname, std::ios::binary | std::ios::in), disk_out(fname, std::ios::binary | std::ios::out) {
		for (uint64_t offset = 0; offset < DISK_SIZE; offset += DEFAULT_BLOCK_SIZE) {
			auto block = std::make_shared<free_block>();
			block->offset = offset;
			block->next = free128;
			free128 = block;
				LOG(INFO) << CLASS_NAME << "First block is... " << offset;
		}
		if (free128 != nullptr) {
			LOG(INFO) << CLASS_NAME << "real First block is... " << free128->offset;
		}
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
			uint64_t new_free_offset = offset + DEFAULT_BLOCK_SIZE / 2;
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
		listMtx.lock();
		if (len <= DEFAULT_BLOCK_SIZE / 2) {
			// Then try to take a free 64 megabyte block, otherwise split a 128m block.
			if (!allocate64(offset)) {
				LOG(ERROR) << CLASS_NAME << "Could not find a free 64-megabyte block.";
				listMtx.unlock();
				return false;
			}
		} else {
			if (!allocate128(offset)) {
				LOG(ERROR) << CLASS_NAME << "Could not find a free 128-megabyte block.";
				listMtx.unlock();
				return false;
			}
		}
		listMtx.unlock();
		LOG(INFO) << CLASS_NAME << "Writing block " << id << " to offset " << offset;
		disk_out.seekp(offset);
		disk_out << blk;
		disk_out.flush();

		block_info info;
		info.blockid = id;
		info.offset = offset;
		info.len = len;

		listMtx.lock();
		blocks.push_back(info);
		listMtx.unlock();

		return true;

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
	* Given an ID, deletes a block. Returns false on error, true otherwise
	**/
	bool NativeFS::rmBlock(uint64_t id)
	{
		std::string fileName;
		listMtx.lock();
		// Find and delete block in mapping
		auto iter = blockMap.find(id);
		if(iter == blockMap.end()){
			LOG(ERROR) << CLASS_NAME << "rmBlock failed: block not found";
			return false;
		}
		fileName = iter->second;
		listMtx.unlock();
		//Copy to a char*, which erase and remove need
		char *fileNameFmtd = const_cast<char*>(fileName.c_str());

		// Delete the corresponding file
		if(remove(fileNameFmtd) != 0 ){
			LOG(ERROR) << CLASS_NAME << "rmBlock failed: error deleting file";
			return false;
		}

		listMtx.lock();
		blockMap.erase(iter);
		listMtx.unlock();
		return true;

	}
}
