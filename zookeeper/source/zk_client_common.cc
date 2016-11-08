#ifndef RDFS_ZKCLIENTCOMMON_CC
#define RDFS_ZKCLIENTCOMMON_CC

#include "../include/zk_client_common.h"

#include <easylogging++.h>
#include <zkwrapper.h>
#include <iostream>

namespace zkclient {

    const std::string ZkClientCommon::WORK_QUEUES = "/work_queues/";
    const std::string ZkClientCommon::WAIT_FOR_ACK = "wait_for_acks";
    const std::string ZkClientCommon::WAIT_FOR_ACK_BACKSLASH = "wait_for_acks/";
    const std::string ZkClientCommon::REPLICATE_BACKSLASH = "replicate/";
    const std::string ZkClientCommon::NAMESPACE_PATH = "/fileSystem";
    const std::string ZkClientCommon::HEALTH =  "/health";
    const std::string ZkClientCommon::HEALTH_BACKSLASH = "/health/";
    const std::string ZkClientCommon::STATS = "/stats";
    const std::string ZkClientCommon::HEARTBEAT = "/heartbeat";
    const std::string ZkClientCommon::CLASS_NAME = ": **ZkNnCommon** : ";
	const std::string ZkClientCommon::BLOCK_LOCATIONS = "/block_locations/";

    ZkClientCommon::ZkClientCommon(std::string hostAndIp) {

        int error_code;
        zk = std::make_shared<ZKWrapper>(hostAndIp, error_code);
        init();
    }

    ZkClientCommon::ZkClientCommon(std::shared_ptr <ZKWrapper> zk_in) : zk(zk_in) {
        init();
    }

    void ZkClientCommon::init() {
        LOG(INFO) << CLASS_NAME <<  "Initializing ZkClientCommon";
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
            LOG(ERROR) << CLASS_NAME <<  "Failed creating /work_queues/wait_for_acks: " << error_code;
        }
        if (zk->exists("/block_locations", exists, error_code)) {
            if (!exists) {
                zk->create("/block_locations", vec, error_code);
            }
        } else {
            // TODO: Handle error
        }

        LOG(INFO) << CLASS_NAME <<  "Finished ZkClientCommon";

    }
}

#endif
