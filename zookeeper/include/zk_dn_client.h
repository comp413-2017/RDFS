// Copyright 2017 Rice University, COMP 413 2017

#ifndef ZOOKEEPER_INCLUDE_ZK_DN_CLIENT_H_
#define ZOOKEEPER_INCLUDE_ZK_DN_CLIENT_H_

#include "zk_client_common.h"
#include <google/protobuf/message.h>
#include <atomic>
#include <string>
#include "hdfs.pb.h"

class TransferServer;

namespace zkclient {

typedef struct {
  std::string ip;
  uint32_t ipcPort;
} DataNodeId;

// TODO(2016): Store hostname in payload as a vararg?
typedef struct {
  uint32_t ipcPort;
  uint32_t xferPort;
  uint64_t disk_bytes;    // total space on disk
  uint64_t free_bytes;    // free space on disk
  uint32_t xmits;            // current number of xmits
} DataNodePayload;

typedef struct {
  char ipPort[256];
} DataNodeZNode;

/**
* A struct used to write the number of bytes in a block (and
* maybe other block-related things in the future) to a ZNode
*/
typedef struct {
  uint64_t block_size;  // size of the block in bytes
} BlockZNode;

/**
* Class representing a Zookeeper-DataNode client.
* Allows a DataNode to update the state of ZK.
*/
class ZkClientDn : public ZkClientCommon {
 public:
  /**
  * Initializes the Zookeeper-Datanode client.
  * @param ip The DataNode IP
  * @param hostname The DataNode hostname
  * @param zkIpAndAddress The IP and hostname of ZK
  * @param ipcPort TODO
  * @param xferPort TODO
  */
  ZkClientDn(const std::string &ip,
             const std::string &zkIpAndAddress,
             uint64_t total_disk_space,
             const uint32_t ipcPort = 50020,
             const uint32_t xferPort = 50010);

  ZkClientDn(const std::string &ip,
             std::shared_ptr<ZKWrapper>,
             uint64_t total_disk_space,
             const uint32_t ipcPort = 50020,
             const uint32_t xferPort = 50010);
  ~ZkClientDn();

  /**
  * Registers this DataNode with Zookeeper.
  */
  void registerDataNode(const std::string &ip,
                        uint64_t total_disk_space,
                        const uint32_t ipcPort,
                        const uint32_t xferPort);

  /**
  * Informs Zookeeper when the DataNode has received a block. Adds an acknowledgment
  * and creates a node for the DN in the block's block_locations.
  * @param uuid The UUID of the block received by the DataNode.
  * @param size_bytes The number of bytes in the block
  * @return True on success, false on error.
  */
  bool blockReceived(uint64_t uuid, uint64_t size_bytes);

  bool sendStats(uint64_t free_space, uint32_t xmits);

  /**
   * Set the transfer server that this dn uses for read/writes
   */
  void setTransferServer(std::shared_ptr<TransferServer> &server);

  /**
  * Push the blockid onto the replication queue belonging to dn_name
  * @param dn_name the queue to add onto
  * @param blockid the id of the block to be replicated
  * @return true if success
  */
  bool push_dn_on_repq(std::string dn_name, uint64_t blockid);

  bool poll_replication_queue();

  bool poll_delete_queue();

  /**
   * Checks the ec_recovery queue for jobs
   */
  bool poll_reconstruct_queue();

  std::string get_datanode_id();

 private:
  std::shared_ptr<TransferServer> server;

  /**
  * Builds a string of the DataNode ID.
  * @param data_node_id The DataNode's DataNodeId object,
  * containing the IP and port.
  * @return The ID string
  */

  void processDeleteQueue();

  std::string build_datanode_id(DataNodeId data_node_id);

  DataNodeId data_node_id;
  DataNodePayload data_node_payload;

  static const std::string CLASS_NAME;

  /**
  * Sets up the work queue for this datanode in zookeeper, and sets the watcher
  * on that queue.  To be used for replication and deletion queues
  * @param queueName the name of the queue, i.e. replication or deletion
  * @param watchFuncPtr the watcher function to be used on the queue
  */
  void initWorkQueue(std::string queueName, void (*watchFuncPtr)(zhandle_t *,
                                                                 int,
                                                                 int,
                                                                 const char *,
                                                                 void *));

  /**
   * Handle all of the work items on path
   */
  void handleReplicateCmds(const std::string &path);

  /**
   * Handle all reconstruct items on path
   */
  bool handleReconstructCmds(const std::string &path);

  static void thisDNDeleteQueueWatcher(zhandle_t *zzh,
                                       int type,
                                       int state,
                                       const char *path,
                                       void *watcherCtx);

  /**
   * Find one datanode that has the block_uuid
   */
  bool find_datanode_with_block(uint64_t &block_uuid,
                                std::string &datanode,
                                int &error_code);

  /**
   * Copied from nn client, create block proto
   */
  bool buildExtendedBlockProto(hadoop::hdfs::ExtendedBlockProto *eb,
                               const std::uint64_t &block_id,
                               const uint64_t &block_size);
};
}  // namespace zkclient

#endif   // ZOOKEEPER_INCLUDE_ZK_DN_CLIENT_H_
