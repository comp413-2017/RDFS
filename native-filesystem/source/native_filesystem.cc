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

namespace nativefs {
	const std::string NativeFS::CLASS_NAME = ": **NativeFS** : ";

	NativeFS::NativeFS() {}

	/**
	* Given an ID, write the given block to the native filesystem. Returns true/false on success/failure.
	**/
	bool NativeFS::writeBlock(uint64_t id, std::string blk)
	{
		mapMtx.lock();
		if (!blockMap[id].empty()) {
			LOG(ERROR) << CLASS_NAME << "writeBlock failed: block with id " << id << " already exists";
			return false;
		}
		mapMtx.unlock();

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

		mapMtx.lock();
		blockMap[id] = filename;
		mapMtx.unlock();
		return true;

	}
	/**
	* Given an ID, returns a block buffer
	**/
	std::string NativeFS::getBlock(uint64_t id, bool& success)
	{
		// Look in map and get filename
		mapMtx.lock();
		std::string strFilename = blockMap[id];
		mapMtx.unlock();
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
		mapMtx.lock();
		// Find and delete block in mapping
		auto iter = blockMap.find(id);
		if(iter == blockMap.end()){
			LOG(ERROR) << CLASS_NAME << "rmBlock failed: block not found";
			return false;
		}
		fileName = iter->second;
		mapMtx.unlock();
		//Copy to a char*, which erase and remove need
		char *fileNameFmtd = const_cast<char*>(fileName.c_str());

		// Delete the corresponding file
		if(remove(fileNameFmtd) != 0 ){
			LOG(ERROR) << CLASS_NAME << "rmBlock failed: error deleting file";
			return false;
		}

		mapMtx.lock();
		blockMap.erase(iter);
		mapMtx.unlock();
		return true;

	}
}
