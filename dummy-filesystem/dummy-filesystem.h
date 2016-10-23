#include <iostream>
#include <fstream>
#include <map>
#include <string>
#include <stdio.h>
#include <stdlib.h>

class DummyFilesystem{
public:
	int allocateBlock(long id, unsigned char* blk);
	unsigned char* getBlock(long id);
	int rmBlock(long id);

private:

};
