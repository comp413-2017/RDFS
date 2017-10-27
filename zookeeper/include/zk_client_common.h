// Copyright 2017 Rice University, COMP 413 2017

#ifndef ZOOKEEPER_INCLUDE_ZK_CLIENT_COMMON_H_
#define ZOOKEEPER_INCLUDE_ZK_CLIENT_COMMON_H_

#include <zkwrapper.h>

#include <string>

#include <boost/shared_ptr.hpp>

namespace zkclient {

class ZkClientCommon {
 public:
  explicit ZkClientCommon(std::string hostAndIp);
  explicit ZkClientCommon(std::shared_ptr<ZKWrapper> zk);

  void init();


  /**
   * true if the highest bit is set to 1. false otherwise.
   * @param block_id the id of a block (or a block group)
   * @return true or false.
   */
  bool is_ec_block(u_int64_t block_id);

  /**
   * Given a block or block group id, returns the path to the corresponding metadata.
   * For a non EC block, it is BLOCK_LOCATIONS + ID
   * For an EC block, it is BLOCK_GROUP_LOCATIONS + ID
   * @param block_or_block_group_id the id of a block or a block group.
   * @return the path to the corresponding metadata.
   */
  std::string get_block_metadata_path(u_int64_t block_or_block_group_id);

  std::shared_ptr<ZKWrapper> zk;

  // constants used by the clients
  static const char NAMESPACE_PATH[];
  static const char HEALTH[];
  static const char HEALTH_BACKSLASH[];
  static const char STATS[];
  static const char HEARTBEAT[];
  static const char WORK_QUEUES[];
  static const char REPLICATE_QUEUES[];
  static const char REPLICATE_QUEUES_NO_BACKSLASH[];
  static const char DELETE_QUEUES[];
  static const char DELETE_QUEUES_NO_BACKSLASH[];
  static const char WAIT_FOR_ACK[];
  static const char WAIT_FOR_ACK_BACKSLASH[];
  static const char REPLICATE_BACKSLASH[];
  static const char BLOCK_LOCATIONS[];
  static const char BLOCK_GROUP_LOCATIONS[];
  static const char BLOCKS[];

 private:
  static const std::string CLASS_NAME;
};
}  // namespace zkclient

#endif  // ZOOKEEPER_INCLUDE_ZK_CLIENT_COMMON_H_
