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

	typedef struct {
		uint64_t blockid;
		uint64_t offset;
		uint32_t len;
	} block_info;

	const size_t MAX_BLOCK_SIZE = 134217728;
	const size_t MIN_BLOCK_SIZE = 134217728 / 2;
	constexpr size_t DISK_SIZE = MAX_BLOCK_SIZE * 6;
	constexpr size_t BLOCK_LIST_LEN = DISK_SIZE / MIN_BLOCK_SIZE;
	constexpr size_t BLOCK_LIST_SIZE = BLOCK_LIST_LEN * sizeof(block_info);
	const std::string MAGIC = "OPNSESME";
	// Reserved space for magic bytes + block_info array.
	constexpr size_t RESERVED_SIZE = sizeof(block_info) * (BLOCK_LIST_LEN) + 8;

class NativeFS{
	public:
		NativeFS(std::string);
		NativeFS(NativeFS& other);
		bool writeBlock(uint64_t, std::string);
		std::string getBlock(uint64_t, bool&);
		bool rmBlock(uint64_t);

	private:
		bool allocate64(uint64_t& offset);
		bool allocate128(uint64_t& offset);
		bool addBlock(const block_info& info);
		void flushBlocks();

		std::array<block_info, BLOCK_LIST_LEN> blocks;
		mutable std::mutex listMtx;
		std::map<uint64_t, std::string> blockMap;
		std::shared_ptr<free_block> free64;
		std::shared_ptr<free_block> free128;
		std::ofstream disk_out;
		std::ifstream disk_in;
		static const std::string CLASS_NAME;

};

}
