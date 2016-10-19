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
        auto vec = ZKWrapper::get_byte_vector("");

        if (zk->exists("/health", 0)) {
            zk->create("/health", vec);
        }
        if (zk->exists("/fileSystem", 0)){
            zk->create("/fileSystem", vec);
        }
        if (zk->exists("/work_queues", 0)){
            zk->create("/work_queues", vec);
        }
        if (zk->exists("/blockMap", 0)) {
            zk->create("/blockMap", vec);
        }
        std::cout << "Finished ZkClientCommon" << std::endl;

    }
}

#endif

