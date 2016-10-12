#ifndef RDFS_ZKCLIENTCOMMON_CC
#define RDFS_ZKCLIENTCOMMON_CC

#include "../include/zk_client_common.h"
#include <zkwrapper.h>
namespace zkclient {
    ZkClientCommon::ZkClientCommon() {
        zk = std::make_shared<ZKWrapper>("localhost:2181");  
	//ZKWrapper zk("localhost:2181");
        // no-op
    }

    void ZkClientCommon::init() {
	/* create health node */
    	if (1){
	        zk->create("/health", "health", 6);
	}
    	
    }
}

#endif

