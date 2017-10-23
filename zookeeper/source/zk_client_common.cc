// Copyright 2017 Rice University, COMP 413 2017

#ifndef RDFS_ZKCLIENTCOMMON_CC
#define RDFS_ZKCLIENTCOMMON_CC

#include "../include/zk_client_common.h"

#include <easylogging++.h>
#include <zkwrapper.h>
#include <iostream>

namespace zkclient {

const char ZkClientCommon::WORK_QUEUES[] = "/work_queues/";
const char ZkClientCommon::REPLICATE_QUEUES[] = "/work_queues/replicate/";
const char ZkClientCommon::
        REPLICATE_QUEUES_NO_BACKSLASH[] = "/work_queues/replicate";
const char ZkClientCommon::DELETE_QUEUES[] = "/work_queues/delete/";
const char ZkClientCommon::DELETE_QUEUES_NO_BACKSLASH[] = "/work_queues/delete";
const char ZkClientCommon::WAIT_FOR_ACK[] = "wait_for_acks";
const char ZkClientCommon::WAIT_FOR_ACK_BACKSLASH[] = "wait_for_acks/";
const char ZkClientCommon::REPLICATE_BACKSLASH[] = "replicate/";
const char ZkClientCommon::NAMESPACE_PATH[] = "/fileSystem";
const char ZkClientCommon::HEALTH[] = "/health";
const char ZkClientCommon::HEALTH_BACKSLASH[] = "/health/";
const char ZkClientCommon::STATS[] = "/stats";
const char ZkClientCommon::HEARTBEAT[] = "/heartbeat";
const char ZkClientCommon::BLOCK_LOCATIONS[] = "/block_locations/";
const char ZkClientCommon::BLOCKS[] = "/blocks";

ZkClientCommon::ZkClientCommon(std::string hostAndIp) {
  int error_code;
  zk = std::make_shared<ZKWrapper>(hostAndIp, error_code, "/testing");
  init();
}

ZkClientCommon::ZkClientCommon(std::shared_ptr<ZKWrapper> zk_in) : zk(zk_in) {
  init();
}

void ZkClientCommon::init() {
  LOG(INFO) << "Initializing ZkClientCommon";
  auto vec = ZKWrapper::get_byte_vector("");

  bool exists;
  int error_code;
  // TODO(2016): Add in error handling for failures
  if (zk->exists("/health", exists, error_code)) {
    if (!exists) {
      zk->create("/health", vec, error_code, false);
    }
  } else {
    // TODO(2016): Handle error
  }
  if (zk->exists("/fileSystem", exists, error_code)) {
    if (!exists) {
      zk->create("/fileSystem", vec, error_code, false);
    } else {
    }
  } else {
    // TODO(2016): Handle error
  }
  if (!zk->recursive_create("/work_queues/wait_for_acks",
                            ZKWrapper::EMPTY_VECTOR, error_code)) {
    LOG(ERROR) << "Failed creating /work_queues/wait_for_acks: " << error_code;
  }
  // Ensure work_queues exist
  if (zk->exists(DELETE_QUEUES_NO_BACKSLASH, exists, error_code)) {
    if (!exists) {
      if (!zk->create(DELETE_QUEUES_NO_BACKSLASH, ZKWrapper::EMPTY_VECTOR,
                      error_code, false)) {
        // Handle failed to create replicate node
        LOG(INFO) << "Creation failed for delete ueue";;
      }
    }
  }
  if (zk->exists(REPLICATE_QUEUES_NO_BACKSLASH, exists, error_code)) {
    if (!exists) {
      if (!zk->create(REPLICATE_QUEUES_NO_BACKSLASH, ZKWrapper::EMPTY_VECTOR,
                      error_code, false)) {
        // Handle failed to create replicate node
        LOG(INFO) << "Creation failed for repl queue";;
      }
    }
  }
  if (zk->exists("/block_locations", exists, error_code)) {
    if (!exists) {
      zk->create("/block_locations", vec, error_code, false);
    }
  } else {
    // TODO(2016): Handle error
  }

  LOG(INFO) << "Finished ZkClientCommon";
}
}  // namespace zkclient

#endif
