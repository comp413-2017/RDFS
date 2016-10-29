#ifndef RDFS_ZKCLIENTCOMMON_CC
#define RDFS_ZKCLIENTCOMMON_CC

#include "../include/zk_client_common.h"

#include <easylogging++.h>
#include <zkwrapper.h>
#include <iostream>

namespace zkclient {
    ZkClientCommon::ZkClientCommon(std::string hostAndIp) {

        int error_code;
        zk = std::make_shared<ZKWrapper>(hostAndIp, error_code);
        init();
    }

    ZkClientCommon::ZkClientCommon(std::shared_ptr <ZKWrapper> zk_in) : zk(zk_in) {
        init();
    }

    void ZkClientCommon::init() {
        /* return 0 if path exists, 1 otherwise. */
        LOG(INFO) << "Initializing ZkClientCommon";
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
            } else {
            }
        } else {
            // TODO: Handle error
        }
        if (!zk->recursive_create("/work_queues/wait_for_acks", ZKWrapper::EMPTY_VECTOR, error_code)) {
            LOG(ERROR) << "Failed creating /work_queues/wait_for_acks: " << error_code;
        }
        /*
        if (zk->exists("/work_queues",  exists, error_code)) {
            if (!exists) {
                zk->create("/work_queues", vec, error_code);
            }
            if (zk->exists("/work_queues/wait_for_acks",  exists, error_code)) {

            }

        } else {
            // TODO: Handle error
        }
         */
        if (zk->exists("/block_locations", exists, error_code)) {
            if (!exists) {
                zk->create("/block_locations", vec, error_code);
            }
        } else {
            // TODO: Handle error
        }

        LOG(INFO) << "Finished ZkClientCommon";

    }
}

#endif

