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
		/**
		 * Construct a NativeFS backed by the given filename.
		 */
		NativeFS(std::string);
		/**
		 * Construct a NativeFS by copying fields from other and moving disk
		 * streams over.
		 */
		NativeFS(NativeFS& other);
		/**
		 * Destroy NativeFS, free block array.
		 */
		~NativeFS();
		/**
		 * Write provided block contents to a block with given id. On success,
		 * return true. Otherwise false (e.g. no free space).
		 */
		bool writeBlock(uint64_t, std::string);
		/**
		 * Get the contents of the block with given id. If successful, return
		 * the contents of the block and set provided bool to true. On failure
		 * to find the block, return empty string and set bool to false.
		 */
		std::string getBlock(uint64_t, bool&);
		/**
		 * Delete contents of provided block id from this datanode. Return true
		 * if delete successful, false otherwise (block id not found).
		 */
		bool rmBlock(uint64_t);
		/**
		 * Return the total capacity of this datanode.
		 */
		long getTotalSpace();
		/**
		 * Return an estimate of the remaining free space on this datanode.
		 */
		long getFreeSpace();

	private:
		/**
		 * Attempt to place provided block info in the block list. Return
		 * whether successful, false if no empty space found in the block list.
		 */
		bool addBlock(const block_info& info);
		/**
		 * Mark the area of disk from start to end as free.
		 */
		void freeRange(uint64_t start, uint64_t end);
		bool allocateBlock(size_t size, uint64_t& offset);
		/**
		 * Persist current block metadata to storage.
		 */
		void flushBlocks();
		/**
		 * For debugging, print the free block lists.
		 */
		void printFreeBlocks();

		block_info* blocks;
		mutable std::mutex listMtx;
		std::vector<std::vector<uint64_t>> freeLists;
		std::ofstream disk_out;
		std::ifstream disk_in;
		static const std::string CLASS_NAME;

};

}
