#include <iostream>
#include <fstream>
#include <map>
#include <string>
#include <stdio.h>
#include <stdlib.h>


// Maps block id to filename where it's being stored
std::map<long, std::string> blockMap;

// Given an ID, allocate a block. Returns a status message
int allocateBlock(long id, unsigned char* blk)
{
	std::string filename = std::to_string(id);
	std::fstream file;
	file.open(filename);

	file << blk;
	file.close();

	return 0;

}
// TODO: Potentially return value if error reading file/allocating space
// TODO: Decide where we're storing files
// Given an ID, returns a block buffer
// unsigned char* getBlock(long id)
// {
// 	// Look in map and get filename
// 	string filename = blockMap[id];
//
// 	// Open file
// 	FILE* file;
// 	file = fopen(filename, "r");
// 	if (file == NULL) {
// 		fputs("Error opening file", stderr);
// 		exit(2);
// 	}
//
// 	// Find file size and allocate enough space
// 	long fileSize = fseek(file, 0, SEEK_END);
// 	rewind(file);
// 	unsigned char* blk = (unsigned char*) malloc(sizeof(unsigned char)*fileSize);
// 	if (blk == NULL) {
// 		fputs("Error allocating memory", stderr)
// 		exit(3);
// 	}
//
// 	// Copy file into buffer
// 	size_t bytesRead = fread(blk, 1, fileSize, file);
// 	if (bytesRead != fileSize) {
// 		fputs("Error reading in file", stderr);
// 		exit(3);
// 	}
//
// 	// Close file
// 	fclose(file);
//
// 	// Return buffer
// 	return blk;
// }

// Given an ID, deletes a block. Returns a status message
int rmBlock(long id)
{
	return 0;
}
