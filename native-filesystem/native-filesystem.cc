#include <iostream>
#include <fstream>
#include <sstream>
#include <map>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include "native-filesystem.h"

namespace nativefs {

NativeFS::NativeFS() {}

/**
 * Given an ID, write the given block to the native filesystem. Returns true/false on success/failure.
 **/
bool NativeFS::writeBlock(uint64_t id, std::string blk)
{

	if (!blockMap[id].empty()) {
		return false;
	}

	std::ostringstream oss;
	oss << id;
	std::string filename = "block" + oss.str() + ".txt";
	std::ofstream myfile (filename);

	// There was an error creating the file
	if (myfile.fail()){
		std::cout << "Error creating file" << std::endl;
		return false;
	}

	myfile << blk;
	myfile.close();

	blockMap[id] = filename;

	return true;

}
/**
 * Given an ID, returns a block buffer
**/
std::string NativeFS::getBlock(uint64_t id, bool& success)
{
	// Look in map and get filename
	std::string strFilename = blockMap[id];
	char* filename = const_cast<char*>(strFilename.c_str());
	const uint64_t blockSize = 134217728;
	// Open file
	FILE* file;
	file = fopen(filename, "r");
	if (file == NULL) {
		std::cout << "Error opening file " << filename << std::endl;
		success = false;
		return "";
	}


	// Find file size and allocate enough space
	std::string blk(blockSize, 0);

	// Copy file into buffer
	size_t bytesRead = fread(&blk[0], sizeof(char), blockSize, file);
	if (bytesRead == 0) {
		fclose(file);
		std::cout << "Error reading in file" << std::endl;
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

	// Find and delete block in mapping
	auto iter = blockMap.find(id);
	if(iter == blockMap.end()){
		std::cout << "Error: block not found" << std::endl;
		return false;
	}
	fileName = iter->second;

	//Copy to a char*, which erase and remove need
	char *fileNameFmtd = const_cast<char*>(fileName.c_str());

	// Delete the corresponding file
	if(remove(fileNameFmtd) != 0 ){
		std::cout << "Error deleting file" << std::endl;
		return false;
	}

	blockMap.erase(iter);

	return true;

}
}
