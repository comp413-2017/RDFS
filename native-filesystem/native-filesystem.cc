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
 * Given an ID, allocate a block. Returns true/false on success/failure.
 **/
bool NativeFS::allocateBlock(long id, std::string blk)
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
unsigned char* NativeFS::getBlock(long id)
{
	// Look in map and get filename
	std::string strFilename = blockMap[id];
	char* filename = const_cast<char*>(strFilename.c_str());
	const long blockSize = 67108864;
	// Open file
	FILE* file;
	file = fopen(filename, "r");
	if (file == NULL) {
		std::cout << "Error opening file " << filename << std::endl;
		return NULL;
	}

	// Find file size and allocate enough space
	unsigned char* blk = (unsigned char*) malloc(sizeof(unsigned char)*blockSize);
	if (blk == NULL) {
		std::cout << "Error allocating memory" << std::endl;
		return NULL;
	}

	// Copy file into buffer
	size_t bytesRead = fread(blk, 1, blockSize, file);
	if (bytesRead == 0) {
		std::cout << "Error reading in file std::endl" << std::endl;
		return NULL;
	}

	// Close file
	fclose(file);

	// Return buffer
	return blk;
}
/**
 * Given an ID, deletes a block. Returns false on error, true otherwise
**/
bool NativeFS::rmBlock(long id)
{
	std::string fileName;

	// Find and delete block in mapping
	auto iter = blockMap.find(id);
	if(iter == blockMap.end()){
		fputs("Error: block not found\n", stderr);
		return false;
	}
	fileName = iter->second;

	//Copy to a char*, which erase and remove need
	char *fileNameFmtd = const_cast<char*>(fileName.c_str());
	blockMap.erase(iter);

	// Delete the corresponding file
	if( remove(fileNameFmtd) != 0 ){
		fputs("Error deleting file\n", stderr);
		return false;
	}
	return true;

}
}
