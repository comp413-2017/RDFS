#include <iostream>
#include <fstream>
#include <map>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <easylogging++.h>
#include <mutex>
#include "block_queue.h"

#pragma once

namespace nativefs {

	typedef struct block_info {
		uint64_t blockid;
		uint64_t offset;
		uint32_t len;
	} block_info;

	const size_t DEFAULT_BLOCK_SIZE = 134217728;
	constexpr size_t DISK_SIZE = DEFAULT_BLOCK_SIZE * 6;

class NativeFS{
	public:
		NativeFS(std::string);
		bool writeBlock(uint64_t, std::string);
		std::string getBlock(uint64_t, bool&);
		bool rmBlock(uint64_t);

		NativeFS(NativeFS& other) :
			free64(other.free64),
			free128(other.free128),
			blocks(other.blocks),
			disk_out(std::move(other.disk_out)),
			disk_in(std::move(other.disk_in)) {}

	private:
		bool allocate64(uint64_t& offset);
		bool allocate128(uint64_t& offset);

		std::vector<block_info> blocks;
		mutable std::mutex listMtx;
		std::map<uint64_t, std::string> blockMap;
		std::shared_ptr<free_block> free64;
		std::shared_ptr<free_block> free128;
		std::ofstream disk_out;
		std::ifstream disk_in;
		static const std::string CLASS_NAME;

};

}
