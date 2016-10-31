#include <iostream>
#include <fstream>
#include <map>
#include <string>
#include <stdio.h>
#include <stdlib.h>


namespace nativefs {

class NativeFS{
	public:
		NativeFS();
		bool allocateBlock(long, std::string);
		std::string getBlock(long);
		bool rmBlock(long);

	private:
		std::map<long, std::string> blockMap;
};

}
