#include <iostream>
#include <fstream>
#include <map>
#include <string>
#include <stdio.h>
#include <stdlib.h>

#pragma once

namespace nativefs {

class NativeFS{
	public:
		NativeFS();
		bool allocateBlock(uint64_t, std::string);
		std::string getBlock(uint64_t, bool&);
		bool rmBlock(uint64_t);

	private:
		std::map<uint64_t, std::string> blockMap;
};

}
