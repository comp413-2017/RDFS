#include <iostream>
#include <fstream>
#include <sstream>
#include <map>
#include <string>
#include <stdio.h>
#include <stdlib.h>


// Maps block id to filename where it's being stored
std::map<long, std::string> blockMap;

// Given an ID, allocate a block. Returns a status message
int allocateBlock(long id, unsigned char* blk)
{

	if (!blockMap[id].empty()) {
		return -1;
	}

	std::ostringstream oss;
	oss << id;
	std::string filename = "block" + oss.str() + ".txt";

	blockMap[id] = filename;
	std::ofstream myfile (filename);

	myfile << blk;
	myfile.close();

	return 0;

}
// TODO: Potentially return value if error reading file/allocating space
// TODO: Decide where we're storing files
// Given an ID, returns a block buffer
unsigned char* getBlock(long id)
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

// Given an ID, deletes a block. Returns -1 on error, 0 otherwise
int rmBlock(long id)
{
	std::string fileName;

	// Find and delete block in mapping
	auto iter = blockMap.find(id);
	if(iter == blockMap.end()){
		fputs("Error: block not found\n", stderr);
		return -1;
	}
	fileName = iter->second;

	//Copy to a char*, which erase and remove need
	char *fileNameFmtd = const_cast<char*>(fileName.c_str());
	blockMap.erase(iter);

	// Delete the corresponding file
	if( remove(fileNameFmtd) != 0 ){
		fputs("Error deleting file\n", stderr);
		return -1;
	}
	return 0;

}
