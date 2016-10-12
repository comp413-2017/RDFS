#ifndef RDFS_ZKCLIENTCOMMON_CC
#define RDFS_ZKCLIENTCOMMON_CC

#include "../include/zk_client_common.h"
#include <zkwrapper.h>
namespace zkclient {
    ZkClientCommon::ZkClientCommon() {
        zk = std::make_shared<ZKWrapper>("localhost:2181");  
    }

    void ZkClientCommon::init() {
        /* return 0 if path exists, 1 otherwise. */
	if (zk->exists("/health"))
	        zk->create("/health", "health", 6);
	if (zk->exists("/fileSystem"))
                zk->create("/fileSystem", "fileSystem", 10);
	if (zk->exists("/work_queues"))
                zk->create("/work_queues", "work_queues", 11);
	if (zk->exists("blockMap"))
                zk->create("/blockMap", "blockMap", 8);
    	
    }
}

#endif

