#ifndef RDFS_ZKCLIENTCOMMON_CC
#define RDFS_ZKCLIENTCOMMON_CC

#include "../include/zk_client_common.h"

#include <zkwrapper.h>
#include <iostream>

namespace zkclient {
    ZkClientCommon::ZkClientCommon(std::string hostAndIp) {

        int error_code;
        zk = std::make_shared<ZKWrapper>(hostAndIp, error_code);
        init();
    }

    void ZkClientCommon::init() {
        /* return 0 if path exists, 1 otherwise. */
        std::cout << "Initializing ZkClientCommon" << std::endl;
        auto vec = ZKWrapper::get_byte_vector("");

        bool exists;
        int error_code;

        // TODO: Add in error handling for failures
        if (zk->exists("/health", exists, error_code)) {
            if (!exists) {
                zk->create("/health", vec, error_code);
            }
        } else {
            // TODO: Handle error
        }
        if (zk->exists("/fileSystem", exists, error_code)) {
            if (!exists) {
                zk->create("/fileSystem", vec, error_code);
            }
        } else {
            // TODO: Handle error
        }
        if (zk->exists("/work_queues",  exists, error_code)) {
            if (!exists) {
                zk->create("/work_queues", vec, error_code);
            }
        } else {
            // TODO: Handle error
        }
        if (zk->exists("/blockMap", exists, error_code)) {
            if (!exists) {
                zk->create("/blockMap", vec, error_code);
            }
        } else {
            // TODO: Handle error
        }

        std::cout << "Finished ZkClientCommon" << std::endl;

    }
}

#endif

