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

class NativeFS{
	public:
		NativeFS();
		bool writeBlock(uint64_t, std::string);
		std::string getBlock(uint64_t, bool&);
		bool rmBlock(uint64_t);

		NativeFS(const NativeFS& other) {
			std::lock_guard<std::mutex> lock(other.mapMtx);
		}
	private:
		std::map<uint64_t, std::string> blockMap;
		static const std::string CLASS_NAME;
		mutable std::mutex mapMtx;

};

}
