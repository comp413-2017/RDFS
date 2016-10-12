#ifndef RDFS_ZKCLIENTCOMMON_CC
#define RDFS_ZKCLIENTCOMMON_CC

#include "../include/zk_client_common.h"

#include <zkwrapper.h>
#include <iostream>

namespace zkclient {
    ZkClientCommon::ZkClientCommon(std::string hostAndIp) {
        zk = std::make_shared<ZKWrapper>(hostAndIp);
        init();
    }

    void ZkClientCommon::init() {
        /* return 0 if path exists, 1 otherwise. */
        std::cout << "Initializing ZkClientCommon" << std::endl;
        if (zk->exists("/health"))
                zk->create("/health", "health", 6);
        if (zk->exists("/fileSystem"))
                    zk->create("/fileSystem", "fileSystem", 10);
        if (zk->exists("/work_queues"))
                    zk->create("/work_queues", "work_queues", 11);
        if (zk->exists("/blockMap"))
                    zk->create("/blockMap", "blockMap", 8);
        std::cout << "Finished ZkClientCommon" << std::endl;

    }
}

#endif

