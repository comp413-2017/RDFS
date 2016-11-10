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

	NativeFS::NativeFS(std::string fname) : disk(fname) {
		for (uint64_t offset = DISK_SIZE; offset >= 0; offset -= DEFAULT_BLOCK_SIZE) {
			auto block = std::make_shared<free_block>();
			block->offset = offset;
			block->next = free128;
			free128 = block;
		}
	}

	/**
	* Given an ID, write the given block to the native filesystem. Returns true/false on success/failure.
	**/
	bool NativeFS::writeBlock(uint64_t id, std::string blk)
	{
		listMtx.lock();
		if (!blockMap[id].empty()) {
			LOG(ERROR) << CLASS_NAME << "writeBlock failed: block with id " << id << " already exists";
			return false;
		}
		listMtx.unlock();

		std::ostringstream oss;
		oss << id;
		std::string filename = "block" + oss.str() + ".txt";
		std::ofstream myfile (filename);

		// There was an error creating the file
		if (myfile.fail()){
			LOG(ERROR) << CLASS_NAME << "writeBlock failed: error creating block file";
			return false;
		}

		myfile << blk;
		myfile.close();

		listMtx.lock();
		blockMap[id] = filename;
		listMtx.unlock();
		return true;

	}
	/**
	* Given an ID, returns a block buffer
	**/
	std::string NativeFS::getBlock(uint64_t id, bool& success)
	{
		// Look in map and get filename
		listMtx.lock();
		std::string strFilename = blockMap[id];
		listMtx.unlock();
		char* filename = const_cast<char*>(strFilename.c_str());
		const uint64_t blockSize = 134217728;
		// Open file
		FILE* file;
		file = fopen(filename, "r");
		if (file == NULL) {
			LOG(ERROR) << CLASS_NAME << "getBlock failed: error opening block file" << strFilename;
			success = false;
			return "";
		}


		// Find file size and allocate enough space
		std::string blk(blockSize, 0);

		// Copy file into buffer
		size_t bytesRead = fread(&blk[0], sizeof(char), blockSize, file);
		if (bytesRead == 0) {
			fclose(file);
			LOG(ERROR) << CLASS_NAME << "getBlock failed: error reading in block";
			success = false;
			return "";
		}

		// No errors if we got this far
		success = true;

		// Close file
		fclose(file);

		// Return buffer
		return blk;
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
