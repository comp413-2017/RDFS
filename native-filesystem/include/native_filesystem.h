#include <iostream>
#include <fstream>
#include <map>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <easylogging++.h>
#include <mutex>

#pragma once

namespace nativefs {

	typedef struct {
		uint64_t blockid;
		uint64_t offset;
		uint32_t len;
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
	const std::string MAGIC = "OPNSESME";
	// Reserved space for magic bytes + block_info array.
	constexpr size_t RESERVED_SIZE = BLOCK_LIST_SIZE + 8;

class NativeFS{
	public:
		NativeFS(std::string);
		NativeFS(NativeFS& other);
		~NativeFS();
		bool writeBlock(uint64_t, std::string);
		std::string getBlock(uint64_t, bool&);
		bool rmBlock(uint64_t);
		long getTotalSpace();
		long getFreeSpace();

	private:
		bool addBlock(const block_info& info);
		/**
		 * Mark the area of disk from start to end as free.
		 */
		void freeRange(uint64_t start, uint64_t end);
		bool allocateBlock(size_t size, uint64_t& offset);
		void flushBlocks();
        void printFreeBlocks();

		block_info* blocks;
		mutable std::mutex listMtx;
		std::vector<std::vector<uint64_t>> freeLists;
		std::ofstream disk_out;
		std::ifstream disk_in;
		static const std::string CLASS_NAME;

};

}
