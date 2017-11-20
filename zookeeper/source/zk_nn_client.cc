// Copyright 2017 Rice University, COMP 413 2017

#ifndef RDFS_ZKNNCLIENT_CC
#define RDFS_ZKNNCLIENT_CC

#include "zk_lock.h"
#include "zk_dn_client.h"
#include "zkwrapper.h"
#include "hdfs.pb.h"
#include "ClientNamenodeProtocol.pb.h"
#include <ConfigReader.h>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem/path.hpp>

#include "zk_lock.h"
#include "zk_dn_client.h"
#include "zk_nn_client.h"
#include "zkwrapper.h"
#include <iostream>
#include <sstream>
#include <ctime>
#include <chrono>
#include <sys/time.h>
#include <easylogging++.h>
#include <google/protobuf/message.h>
#include <erasurecoding.pb.h>
#include <zk_dn_client.h>


using hadoop::hdfs::AddBlockRequestProto;
using hadoop::hdfs::AddBlockResponseProto;
using hadoop::hdfs::AbandonBlockRequestProto;
using hadoop::hdfs::AbandonBlockResponseProto;
using hadoop::hdfs::ExtendedBlockProto;
using hadoop::hdfs::GetFileInfoRequestProto;
using hadoop::hdfs::GetFileInfoResponseProto;
using hadoop::hdfs::HdfsFileStatusProto;
using hadoop::hdfs::HdfsFileStatusProto_FileType;
using hadoop::hdfs::DeleteRequestProto;
using hadoop::hdfs::DeleteResponseProto;
using hadoop::hdfs::CreateRequestProto;
using hadoop::hdfs::CreateResponseProto;
using hadoop::hdfs::CompleteRequestProto;
using hadoop::hdfs::CompleteResponseProto;
using hadoop::hdfs::RenameRequestProto;
using hadoop::hdfs::RenameResponseProto;
using hadoop::hdfs::MkdirsRequestProto;
using hadoop::hdfs::MkdirsResponseProto;
using hadoop::hdfs::GetListingRequestProto;
using hadoop::hdfs::GetListingResponseProto;
using hadoop::hdfs::DirectoryListingProto;
using hadoop::hdfs::LocatedBlocksProto;
using hadoop::hdfs::GetBlockLocationsRequestProto;
using hadoop::hdfs::GetBlockLocationsResponseProto;
using hadoop::hdfs::LocatedBlockProto;
using hadoop::hdfs::GetContentSummaryRequestProto;
using hadoop::hdfs::GetContentSummaryResponseProto;
using hadoop::hdfs::ContentSummaryProto;
using hadoop::hdfs::FsPermissionProto;
using hadoop::hdfs::DatanodeInfoProto;
using hadoop::hdfs::DatanodeIDProto;
using hadoop::hdfs::SetOwnerRequestProto;
using hadoop::hdfs::SetOwnerResponseProto;
using hadoop::hdfs::SetPermissionRequestProto;
using hadoop::hdfs::SetPermissionResponseProto;
using hadoop::hdfs::AclEntryProto;
using hadoop::hdfs::ModifyAclEntriesRequestProto;
using hadoop::hdfs::ModifyAclEntriesResponseProto;
using hadoop::hdfs::ErasureCodingPolicyProto;
using hadoop::hdfs::ECSchemaProto;
using hadoop::hdfs::GetErasureCodingPoliciesRequestProto;
using hadoop::hdfs::GetErasureCodingPoliciesResponseProto;
using hadoop::hdfs::StorageTypeProto;

namespace zkclient {

void ZkNnClient::populateDefaultECProto() {
  DEFAULT_EC_SCHEMA.set_parityunits(DEFAULT_PARITY_UNITS);
  DEFAULT_EC_SCHEMA.set_dataunits(DEFAULT_DATA_UNITS);
  DEFAULT_EC_SCHEMA.set_codecname(DEFAULT_EC_CODEC_NAME);
  RS_SOLOMON_PROTO.set_name(DEFAULT_EC_POLICY);
  RS_SOLOMON_PROTO.set_allocated_schema(&DEFAULT_EC_SCHEMA);
  RS_SOLOMON_PROTO.set_cellsize(DEFAULT_EC_CELLCIZE);
  RS_SOLOMON_PROTO.set_id(DEFAULT_EC_ID);
}

/**
 * A simple print function that will be triggered when
 * namenode loses a heartbeat
 */
void notify_delete() {
  printf("No heartbeat, no childs to retrieve\n");
}

/**
 * Registers watches on health nodes; essentially mimicking a heartbeat
 * liveness check.
 */
void ZkNnClient::register_watches() {
  int err;
  std::vector<std::string> children = std::vector<std::string>();
  if (!(zk->wget_children(HEALTH, children, watcher_health, this, err))) {
    // TODO(2016): Handle error
    LOG(ERROR) << "[register_watchers], wget failed " << err;
  }

  for (int i = 0; i < children.size(); i++) {
    LOG(INFO) << "[register_watches] Attaching child to "
              << children[i]
              << ", ";
    std::vector<std::string> ephem = std::vector<std::string>();
    if (!(zk->wget_children(HEALTH_BACKSLASH + children[i], ephem,
                watcher_health_child, this, err))) {
      // TODO(2016): Handle error
      LOG(ERROR) << "[register_watchers], wget failed " << err;
    }
  }
}

/**
 * 
 */
void ZkNnClient::watcher_health(zhandle_t *zzh, int type, int state,
                const char *path, void *watcherCtx) {
  LOG(INFO) << "[watcher_health] Health watcher triggered on " << path;

  ZkNnClient *cli = reinterpret_cast<ZkNnClient *>(watcherCtx);
  auto zk = cli->zk;

  int err;
  std::vector<std::string> children = std::vector<std::string>();
  if (!(zk->wget_children(HEALTH, children, ZkNnClient::watcher_health,
              watcherCtx, err))) {
    // TODO(2016): Handle error
    LOG(ERROR) << "[watcher_health], wget failed " << err;
  }

  for (int i = 0; i < children.size(); i++) {
    LOG(INFO) << "[watcher_health] Attaching child to "
          << children[i]
          << ", ";
    std::vector<std::string> ephem = std::vector<std::string>();
    if (!(zk->wget_children(HEALTH_BACKSLASH + children[i],
                ephem,
                ZkNnClient::watcher_health_child,
                watcherCtx,
                err))) {
      // TODO(2016): Handle error
      LOG(ERROR) << "[watcher_health], wget failed " << err;
    }
  }
}

/**
* Static
*/
void ZkNnClient::watcher_health_child(zhandle_t *zzh, int type, int state,
                    const char *raw_path, void *watcherCtx) {
  ZkNnClient *cli = reinterpret_cast<ZkNnClient *>(watcherCtx);
  auto zk = cli->zk;
  auto path = zk->removeZKRoot(std::string(raw_path));

  std::vector<std::string> children;

  LOG(INFO) << "[watcher_health_child] Watcher triggered on path '" << path;
  int err, rc;
  bool ret;
  std::vector<std::string> split_path;
  boost::split(split_path, path, boost::is_any_of("/"));
  auto dn_id = split_path[split_path.size() - 1];
  const std::string &repl_q_path = zkclient::ZkClientCommon::REPLICATE_QUEUES
                   + dn_id;
  const std::string &delete_q_path = zkclient::ZkClientCommon::DELETE_QUEUES
      + dn_id;
  const std::string &ec_rec_q_path = zkclient::ZkClientCommon::EC_RECOVER_QUEUES
      + dn_id;
  const std::string &block_path = util::concat_path(
      path,
      zkclient::ZkClientCommon::BLOCKS);
  std::vector<std::uint8_t> bytes;
  std::vector<std::string> to_replicate;
  std::vector<std::string> to_ec_recover;
  /* Lock the znode */
  ZKLock race_lock(*zk.get(), std::string(path));
  if (race_lock.lock()) {
    // Lock failed somehow
    LOG(ERROR) << "[watcher_health_child] An error occurred while "
            "trying to lock " << path;
    return;
  }

  ret = zk->get_children(path, children, err);

  if (!ret) {
    LOG(ERROR) << "[watcher_health_child] Failed to get children";
    goto unlock;
  }

  /* Check for heartbeat */
  for (int i = 0; i < children.size(); i++) {
    if (children[i] == "heartbeat") {
      /* Heartbeat exists! Don't do anything */
      LOG(INFO) << "[watcher_health_child] Heartbeat found while deleting";
      goto unlock;
    }
  }
  children.clear();

  /* Get all replication queue items for this datanode, save it */
  if (!zk->get_children(repl_q_path.c_str(), children, err)) {
    LOG(ERROR) << "[watcher_health_child] Failed to get dead datanode's "
            "replication queue";
    goto unlock;
  }
  for (auto child : children) {
    to_replicate.push_back(child);
  }
  children.clear();

  /* Get all ec_recover queue items for this datanode, save it */
  if (!zk->get_children(ec_rec_q_path.c_str(), children, err)) {
    LOG(ERROR) << "[watcher_health_child] Failed to get dead "
            "datanode's ec_recover queue";
    goto unlock;
  }
  for (auto child : children)
    to_ec_recover.push_back(child);
  children.clear();

  /* Delete all work queues for this datanode */
  if (!zk->recursive_delete(repl_q_path, err)) {
    LOG(ERROR) << "[watcher_health_child] Failed to delete "
            "dead datanode's replication queue";
    goto unlock;
  }
  if (!zk->recursive_delete(delete_q_path, err)) {
    LOG(ERROR) << "[watcher_health_child] Failed to delete "
            "dead datanode's delete queue";
    goto unlock;
  }
  if (!zk->recursive_delete(ec_rec_q_path, err)) {
    LOG(ERROR) << "[watcher_health_child] Failed to delete "
            "dead datanode's ec_recover queue";
    goto unlock;
  }

  /* Put every block from /blocks on replication queue as well as any saved
   * items from replication queue
   */

  if (!zk->get_children(block_path.c_str(), children, err)) {
    LOG(ERROR) << "[watcher_health_child] Failed to get children "
            "for blocks";
    goto unlock;
  }

  /* Push blocks this datanode has onto replication to-queue list */
  for (auto child : children) {
    std::uint64_t block_id;
    std::stringstream strm(child);
    strm >> block_id;
    if (ZkClientCommon::is_ec_block(block_id))
      to_ec_recover.push_back(child);
    else
      to_replicate.push_back(child);
  }
  children.clear();

  /* Delete this datanode. */
  if (!zk->recursive_delete(std::string(path), err)) {
    LOG(ERROR) << "[watcher_health_child] Failed to clean up dead "
            "datanode.";
  }

  /* Push all blocks needing to be replicated onto the queue */
  if (!cli->replicate_blocks(to_replicate, err)) {
    LOG(ERROR) << "[watcher_health_child] Failed to push all items "
            "on to replication queues!";
    goto unlock;
  }
  /* Push all ec blocks needing to be recovered onto the queue */
  if (!cli->recover_ec_blocks(to_ec_recover, err)) {
    LOG(ERROR) << "[watcher_health_child] Failed to push all items "
            "on to ec_recover queues!";
    goto unlock;
  }
  unlock:
  /* Unlock the znode */
  if (race_lock.unlock()) {
    LOG(ERROR) << "[watcher_health_child] An error occurred while "
            "trying to unlock " << path;
  }
}


void ZkNnClient::watcher_listing(zhandle_t *zzh,
                                 int type,
                                 int state,
                                 const char *path,
                                 void *watcherCtx) {
  LOG(INFO) << "[watcher_listing] Listing watcher triggered "
          "on " << path;

  ZkNnClient *cli = reinterpret_cast<ZkNnClient *>(watcherCtx);
  auto zk = cli->zk;
  auto dir = std::string(zkclient::ZkClientCommon::NAMESPACE_PATH);
  auto src = zk->removeZKRootAndDir(dir, std::string(path));
  LOG(INFO) << "[watcher_listing] Removing path " << src;
  // Remove cache entry for this path
  cli->cache->remove(src);
}

bool ZkNnClient::file_exists(const std::string &path) {
  // Initialize to false in case exists fails.
  bool exists = false;
  int error_code;
  zk->exists(ZookeeperFilePath(path), exists, error_code);
  return exists;
}

bool ZkNnClient::get_block_size(const u_int64_t &block_id,
                                uint64_t &blocksize) {
  int error_code;
  std::string block_path = get_block_metadata_path(block_id);

  BlockZNode block_data;
  std::vector<std::uint8_t> data(sizeof(block_data));
  if (!zk->get(block_path, data, error_code)) {
    LOG(ERROR) << "[get_block_size] We could not read "
            "the block at " << block_path;
    return false;
  }
  std::uint8_t *buffer = &data[0];
  memcpy(&block_data, &data[0], sizeof(block_data));

  blocksize = block_data.block_size;
  LOG(INFO) << "[get_block_size] Block size of: " << block_path << " is "
            << blocksize;
  return true;
}

void ZkNnClient::set_node_policy(char policy) {
  ZkNnClient::policy = policy;
}

char ZkNnClient::get_node_policy() {
  return ZkNnClient::policy;
}

bool ZkNnClient::cache_contains(const std::string &path) {
    return cache->contains(path);
}

int ZkNnClient::cache_size() {
    return cache->currentSize();
}
// --------------------------- PROTOCOL CALLS -------------------------------
void ZkNnClient::renew_lease(RenewLeaseRequestProto &req,
                             RenewLeaseResponseProto &res) {
  std::string client_name = req.clientname();
  bool exists;
  int error_code;
  if (!zk->exists(ClientZookeeperPath(client_name), exists, error_code)) {
    LOG(ERROR) << "[renew_lease] Failed to check whether " <<
      ClientZookeeperPath(client_name) << " exits.";
  } else {
    ClientInfo clientInfo;
    clientInfo.timestamp = current_time_ms();
    std::vector<std::uint8_t> data(sizeof(clientInfo));
    znode_data_to_vec(&clientInfo, data);

    if (!exists) {
      if (!zk->create(ClientZookeeperPath(client_name),
                      data, error_code, false)) {
        LOG(ERROR) << "[renew_lease] Failed to create zk path " <<
          ClientZookeeperPath(client_name) << ".";
      }
    } else {
      if (!zk->set(ClientZookeeperPath(client_name), data, error_code)) {
        LOG(ERROR) << "[renew_lease] Failed to set data for " <<
          ClientZookeeperPath(client_name) << ".";
      }
    }
  }
}

void ZkNnClient::recover_lease(RecoverLeaseRequestProto &req,
                               RecoverLeaseResponseProto &res) {
  std::string client_name = req.clientname();
  std::string file_path = req.src();
  int error_code;
  bool exists;
  if (!zk->exists(ZookeeperFilePath(file_path), exists, error_code)) {
    LOG(ERROR) << "[recover_lease] Failed to check whether " <<
      ZookeeperFilePath(file_path) << " exists.";
    res.set_result(false);
    return;
  }
  if (!exists) {
    // TODO(elfyingying): not sure how to throw IOException.
    res.set_result(false);
    return;
  }
  std::vector<std::string> children;
  if (!zk->get_children(LeaseZookeeperPath(file_path), children, error_code)) {
    LOG(ERROR) << "[recover_lease] Failed to get children of " <<
      LeaseZookeeperPath(file_path) << ".";
    res.set_result(false);
    return;
  }
  if (children.size() > 0) {
    for (std::string child : children) {
      LOG(ERROR) << "[recover_lease] Child: " + child;
    }
    LOG(ERROR) << "[recover_lease] Fatal error: there are more than one "
            "leases for "
    << ZookeeperFilePath(file_path) << ".";
    res.set_result(false);
    return;
  }
  if (children.size() == 0) {
    // Currently there is no client holding a lease for this file.
    res.set_result(true);
    return;
  }
  std::string lease_holder_client = children[0];
  if (lease_expired(lease_holder_client)) {
    // TODO(elfyingying): start the lease recovery process
    // that closes the file for the
    // client and make sure the replicas are consistent.
    if (!zk->delete_node(LeaseZookeeperPath(file_path) + '/' +
                           lease_holder_client, error_code, true)) {
      LOG(ERROR) << "[recover_lease] Failed to delete the dead lease holder "
                            "client for "
                    + LeaseZookeeperPath(file_path)
                    + '/' + lease_holder_client + ".";
      res.set_result(false);
      return;
    }
    res.set_result(true);
    return;
  } else {
    // The file is under construction and the lease holder is still alive.
    res.set_result(false);
    return;
  }
}

void ZkNnClient::read_file_znode(FileZNode &znode_data,
                                 const std::string &path) {
  int error_code;
  std::vector<std::uint8_t> data(sizeof(znode_data));
  if (!zk->get(ZookeeperFilePath(path), data, error_code)) {
    LOG(ERROR) << "[read_file_znode] We could not read the file znode "
            "at " << path;
    return;  // don't bother reading the data
  }
  std::uint8_t *buffer = &data[0];
  memcpy(&znode_data, buffer, sizeof(znode_data));
}

void ZkNnClient::file_znode_struct_to_vec(FileZNode *znode_data,
                      std::vector<std::uint8_t> &data) {
  memcpy(&data[0], znode_data, sizeof(*znode_data));
}

template <class T>
void ZkNnClient::znode_data_to_vec(T *znode_data,
                                   std::vector<std::uint8_t> &data) {
  memcpy(&data[0], znode_data, sizeof(*znode_data));
}

template <class T>
void ZkNnClient::read_znode_data(T &znode_data, const std::string &path) {
  int error_code;
  std::vector<std::uint8_t> data(sizeof(znode_data));
  if (!zk->get(path, data, error_code)) {
    LOG(ERROR) << "[read_znode_data] We could not read znode data at " << path;
    return;  // don't bother reading the data
  }
  std::uint8_t *buffer = &data[0];
  memcpy(&znode_data, buffer, sizeof(znode_data));
}

bool ZkNnClient::previousBlockComplete(uint64_t prev_id) {
  if (prev_id == 0) {  // first block, so just say hell yea
    return true;
  }
  int error_code;
  /* this value will eventually be read from config file */
  int MIN_REPLICATION = 1;
  std::vector<std::string> children;
  std::string block_metadata_path = get_block_metadata_path(prev_id);
  if (zk->get_children(block_metadata_path, children, error_code)) {
    if (children.size() >= MIN_REPLICATION) {
      return true;
    } else {
      LOG(INFO) << "[previousBlockComplete] Had to sync: previous block failed";
      // If we failed initially attempt to sync the changes, then check again
      zk->flush(zk->prepend_zk_root(block_metadata_path), true);
      if (zk->get_children(block_metadata_path, children, error_code)) {
        return children.size() >= MIN_REPLICATION;
      }
    }
  }
  return false;
}

bool ZkNnClient::checkAccess(std::string username, FileZNode &znode_data) {
  if (!isSecureMode) {
    return true;
  }
  for (unsigned i = 0; i < 20; i++) {
    if (username.compare(std::string(znode_data.permissions[i])) == 0) {
      return true;
    }
  }
  return false;
}

bool ZkNnClient::set_owner(SetOwnerRequestProto &req,
                           SetOwnerResponseProto &res,
                           std::string client_name) {
  int zk_error;
  FileZNode znode_data;
  const std::string &path = req.src();
  const std::string &username = req.username();

  if (!file_exists(path)) {
    LOG(ERROR) << "[set_owner] Requested path " << path << " does not exist.";
    return false;
  }

  read_file_znode(znode_data, path);

  // Check access
  if (!checkAccess(client_name, znode_data)) {
    LOG(ERROR) << "[set_owner] Access denied to path " << path;
    return false;
  }

  // The first string in the permissions ACL is the file owner
  snprintf(znode_data.permissions[0], MAX_USERNAME_LEN, username.c_str());

  // Serialize struct to byte vector
  std::vector<std::uint8_t> zk_data(sizeof(FileZNode));
  file_znode_struct_to_vec(&znode_data, zk_data);

  // Write the modified node back to Zookeeper
  zk->set(ZookeeperFilePath(path), zk_data, zk_error);

  if (zk_error != ZK_ERRORS::OK) {
    LOG(ERROR) << "[set_owner] ZK reported error writing modified node "
            "back to disk";
    return false;
  }

  return true;
}

bool ZkNnClient::add_block(AddBlockRequestProto &req,
                           AddBlockResponseProto &res,
                           std::string client_name) {
  // make sure previous addBlock operation has completed
  auto prev_id = req.previous().blockid();
  if (!previousBlockComplete(prev_id)) {
    LOG(ERROR) << "[add_block] Previous Add Block Operation has not finished";
    return false;
  }

  // Build a new block for the response
  auto block = res.mutable_block();

  // TODO(may): Make sure we are appending / not appending ZKPath at every step
  const std::string file_path = req.src();

  LOG(INFO) << "[add_block] Attempting to add block to existing file "
            << file_path;

  FileZNode znode_data;
  if (!file_exists(file_path)) {
    LOG(ERROR) << "[add_block] Requested file " << file_path <<
                                                             " does not exist";
    return false;
  }
  read_file_znode(znode_data, file_path);

  // Check access
  if (!checkAccess(client_name, znode_data)) {
    LOG(ERROR) << "[add_block] Access denied to path " << file_path;
    return false;
  }


  // Assert that the znode we want to modify is a file
  if (znode_data.file_type != FileType::File) {
    LOG(ERROR) << "[add_block] Requested file " << file_path << " is not a file";
    return false;
  }

  uint32_t replication_factor = znode_data.replication;
  uint64_t block_size = znode_data.blocksize;
  assert(block_size > 0);

  std::uint64_t block_id;
  auto data_nodes = std::vector<std::string>();
  uint64_t block_group_id;
  std::vector<char> block_indices;

  if (!znode_data.isEC) {
      add_block(file_path, block_id, data_nodes, replication_factor);
  } else {  // case when some EC policy is used.
      add_block_group(
          file_path, block_group_id, data_nodes, block_indices,
          DEFAULT_PARITY_UNITS + DEFAULT_DATA_UNITS);
  }

  // TODO(nate): this might be seriously wrong.
  block->set_offset(0);
  block->set_corrupt(false);

  buildExtendedBlockProto(block->mutable_b(), block_id, block_size);

  // Populate optional fields for an EC block.
  // i.e. block indices and storage IDs.
  if (znode_data.isEC) {
    // Add storage IDs for an EC block.
    for (int i = 0; i < DEFAULT_DATA_UNITS + DEFAULT_PARITY_UNITS; i++) {
      block->set_storageids(i, DEFAULT_STORAGE_ID);
    }

    // Add block indices for an EC block.
    // Each byte (i.e. char) represents an index into the group.
    std::string block_index_string;
    for (int i = 0; i < DEFAULT_DATA_UNITS + DEFAULT_PARITY_UNITS; i++) {
      block_index_string.push_back(block_indices[i]);
    }
    block->set_blockindices(block_index_string);

    // Add storage types for an EC block.
    for (int i = 0; i < DEFAULT_DATA_UNITS + DEFAULT_PARITY_UNITS; i++) {
      block->set_storagetypes(i, StorageTypeProto::DISK);
    }
  }

  for (auto data_node : data_nodes) {
    buildDatanodeInfoProto(block->add_locs(), data_node);
  }

  // Construct security token.
  buildTokenProto(block->mutable_blocktoken());
  return true;
}

/**
 * Since the names were a bit strange and it was a pain to go back and figure out
 * where these were again, I'm writing what the proto fields are here.
 *
 * message has:
 * required ExtendedBlockProto b = 1;
 * required string src = 2;
 * required string holder = 3;
 * optional uint64 fileId = 4 [default = 0];  // default to GRANDFATHER_INODE_ID
 *
 * ExtendedBlockProto has:
 * required string poolId = 1;   // Block pool id - gloablly unique across clusters
 * required uint64 blockId = 2;  // the local id within a pool
 * required uint64 generationStamp = 3;
 * optional uint64 numBytes = 4 [default = 0];  // len does not belong in ebid
*/
bool ZkNnClient::abandon_block(AbandonBlockRequestProto &req,
                               AbandonBlockResponseProto &res,
                               std::string client_name) {
  const std::string &file_path = req.src();
  // I believe this is the lease holder?
  const std::string &holder = req.holder();

  // uint64 generationStamp (as well as the optional uint64 numBytes)
  const ExtendedBlockProto &block = req.b();
  const std::string poolId = block.poolid();
  const uint64_t blockId = block.blockid();
  const uint64_t generation_stamp = block.generationstamp();
  LOG(INFO) << "[abandon_block] Requested to abandon block: " << blockId;
  LOG(INFO) << "[abandon_block] This block is in pool " << poolId;
  LOG(INFO) << "[abandon_block] Its generation stamp is " << generation_stamp;

  const std::string block_id_str = std::to_string(blockId);

  // Double check the file exists first
  FileZNode znode_data;
  if (!file_exists(file_path)) {
    LOG(ERROR) << "[abandon_block] Requested file "
               << file_path << " does not exist";
    return false;
  }
  read_file_znode(znode_data, file_path);

  // Check access
  if (!checkAccess(client_name, znode_data)) {
    LOG(ERROR) << "[abandon_block] Access denied to path " << file_path;
    return false;
  }

  // Assert that the znode we want to modify is a file
  if (znode_data.file_type != FileType::File) {
    LOG(ERROR) << "[abandon_block] Requested file " << file_path << " is not a file";
    return false;
  }
  LOG(INFO) << "[abandon_block] File exists. Building multi op "
          "to abandon block";

  // Find the last block
  int error_code;
  std::vector<std::string> sorted_fs_znodes;
  if (!zk->get_children(ZookeeperBlocksPath(file_path),
                        sorted_fs_znodes, error_code)) {
    LOG(ERROR) << "[abandon_block] Failed getting children of "
               << ZookeeperBlocksPath(file_path)
               << " with error: "
               << error_code;
  }
  std::sort(sorted_fs_znodes.begin(), sorted_fs_znodes.end());

  // Build the multi op - it's a reverse of the one's in add_block.
  // Note that it was due to the ZOO_SEQUENCE flag that this first
  // znode's path has the 9 digit number on the end.
  auto undo_seq_file_block_op = zk->
          build_delete_op(ZookeeperBlocksPath(file_path +
      "/" + sorted_fs_znodes.back()));
  auto undo_ack_op = zk->build_delete_op("/work_queues/wait_for_acks/"
                                             + block_id_str);

  std::vector<std::string> datanodes;
  if (!find_all_datanodes_with_block(blockId, datanodes, error_code)) {
    LOG(ERROR) << "[abandon_block] Could not find datandoes with the block.";
  }

  std::vector<std::shared_ptr<ZooOp>> ops;
  ops.push_back(undo_seq_file_block_op);
  ops.push_back(undo_ack_op);

  std::vector<uint8_t> block_vec(sizeof(uint64_t));
  memcpy(&block_vec[0], &blockId, sizeof(uint64_t));

  // push delete commands onto ops
  for (auto &dn : datanodes) {
    auto delete_queue = util::concat_path(DELETE_QUEUES, dn);
    auto delete_item = util::concat_path(delete_queue, "block-");
    ops.push_back(zk->build_create_op(delete_item, block_vec, ZOO_SEQUENCE));
    blockDeleted(blockId, dn);
  }

  auto results = std::vector<zoo_op_result>();
  int err;

  LOG(INFO) << "[abandon_block] Built multi op. Executing multi "
          "op to abandon block... "
            << file_path;
  if (!zk->execute_multi(ops, results, err)) {
    LOG(ERROR) << "[abandon_block] Failed to write the abandon_block multiop, "
        "ZK state was not changed";
    ZKWrapper::print_error(err);
    for (int i = 0; i < results.size(); i++) {
      LOG(ERROR) << "[abandon_block] \t MULTIOP #"
                 << i << " ERROR CODE: " << results[i].err;
    }
    return false;
  }
  return true;
}

ZkNnClient::GetFileInfoResponse ZkNnClient::get_info(
  GetFileInfoRequestProto &req,
  GetFileInfoResponseProto &res,
  std::string client_name) {
  const std::string &path = req.src();

  if (file_exists(path)) {
    // read the node into the file node struct
    FileZNode znode_data;
    read_file_znode(znode_data, path);

    // Check access
    if (!checkAccess(client_name, znode_data)) {
      LOG(ERROR) << "[get_info] Access denied to path " << path;
      return GetFileInfoResponse::FileAccessRestricted;
    }

    // set the file status in the get file info response res
    HdfsFileStatusProto *status = res.mutable_fs();

    set_file_info(status, path, znode_data);
    LOG(INFO) << "[get_info] Got info for file ";
    return GetFileInfoResponse::Ok;
  } else {
    LOG(INFO) << "[get_info] No file to get info for";
    return GetFileInfoResponse::FileDoesNotExist;
  }
}

/**
 * Create a node in zookeeper corresponding to a file
 */
bool ZkNnClient::create_file_znode(const std::string &path,
                                  FileZNode *znode_data) {
  int error_code;
  if (!file_exists(path)) {
    LOG(INFO) << "[create_file_znode] Creating file znode at " << path;
    {
      LOG(INFO) << "[create_file_znode] is this file ec? "
                << znode_data->isEC << "\n";
      LOG(INFO) << "[create_file_znode] " << znode_data->replication;
      LOG(INFO) << "[create_file_znode] " << znode_data->owner;
      LOG(INFO) << "[create_file_znode] size of znode is "
                << sizeof(*znode_data);
    }
    // serialize struct to byte vector
    std::vector<std::uint8_t> data(sizeof(*znode_data));
    file_znode_struct_to_vec(znode_data, data);

    // create the node in zookeeper
    if (!zk->create(ZookeeperFilePath(path), data, error_code, false)) {
      LOG(ERROR) << "[create_file_znode] ZK create failed with error code "
                 << error_code;
      return false;
    }

    if (znode_data->file_type == FileType::File) {
      std::vector<std::uint8_t> data2(sizeof(*znode_data));
      if (!zk->create(ZookeeperBlocksPath(path), data2, error_code, false)) {
        LOG(ERROR) << "[create_file_znode] ZK create failed with error code "
                   << error_code;
        return false;
      }
      // create the lease branch
      if (!zk->create(LeaseZookeeperPath(path), ZKWrapper::EMPTY_VECTOR,
                      error_code, false)) {
        LOG(ERROR) << "[create_file_znode] ZK create lease file path failed"
                   << error_code;
        return false;
      }
    }
    return true;
  }
  return false;
}

bool ZkNnClient::blockDeleted(uint64_t uuid, std::string id) {
  int error_code;
  bool exists;

  LOG(INFO) << "[blockDeleted] DataNode deleted a block with UUID "
            << std::to_string(uuid);

  auto ops = std::vector<std::shared_ptr<ZooOp>>();
  std::string block_metadata_path = get_block_metadata_path(uuid);
  // Delete block locations
  if (zk->exists(block_metadata_path + "/" + id, exists, error_code)) {
    if (exists) {
      ops.push_back(zk->build_delete_op(block_metadata_path + "/" + id));
      std::vector<std::string> children = std::vector<std::string>();
      if (!zk->get_children(block_metadata_path, children, error_code)) {
        LOG(ERROR) << "[blockDeleted] getting children failed";
      }
      // If deleting last child of block locations,
      // delete block locations at this block uuid
      if (children.size() == 1) {
        ops.push_back(zk->build_delete_op(block_metadata_path));
      }
    }
  }

  // Delete blocks
  if (zk->exists(HEALTH_BACKSLASH + id + BLOCKS +
           "/" + std::to_string(uuid), exists, error_code)) {
    if (exists) {
      ops.push_back(zk->build_delete_op(HEALTH_BACKSLASH + id + BLOCKS +
                        "/" + std::to_string(uuid)));
    }
  }

  std::vector<zoo_op_result> results = std::vector<zoo_op_result>();
  if (!zk->execute_multi(ops, results, error_code)) {
    LOG(ERROR) << "[blockDeleted] Failed multiop when deleting block"
               << std::to_string(uuid);
    for (int i = 0; i < results.size(); i++) {
      LOG(ERROR) << "[blockDeleted] \t MULTIOP #" << i << " ERROR CODE: "
                 << results[i].err;
    }
    return false;
  }
  return true;
}


ZkNnClient::DeleteResponse ZkNnClient::destroy_helper(const std::string &path,
                                std::vector<std::shared_ptr<ZooOp>> &ops) {
  LOG(INFO) << "[destroy_helper] Destroying " << path;

  if (!file_exists(path)) {
    LOG(ERROR) << "[destroy_helper] " << path << " does not exist";
    return DeleteResponse::FileDoesNotExist;
  }

  int error_code;
  FileZNode znode_data;
  read_file_znode(znode_data, path);
  std::vector<std::string> children;
  LOG(INFO) << "read file znode successful";
  if (znode_data.file_type == FileType::Dir) {
    if (!zk->get_children(ZookeeperFilePath(path), children, error_code)) {
          LOG(FATAL) << "[destroy_helper] Failed to get children for " << path;
          return DeleteResponse::FailedChildRetrieval;
    }
    for (auto& child : children) {
      auto child_path = util::concat_path(path, child);
      auto status = destroy_helper(child_path, ops);
      if (status != DeleteResponse::Ok) {
        return status;
      }
    }
  } else if (znode_data.file_type == FileType::File) {
    if (!zk->get_children(ZookeeperBlocksPath(path), children, error_code)) {
      LOG(FATAL) << "[destroy_helper] Failed to get children for " << path;
      return DeleteResponse::FailedChildRetrieval;
    }

    if (znode_data.file_status == FileStatus::UnderConstruction) {
      LOG(ERROR) << path << "[destroy_helper] is under construction, 
            so it cannot be deleted.";
      return DeleteResponse::FileUnderConstruction;
    }
    for (auto &child : children) {
      auto child_path = util::concat_path(ZookeeperBlocksPath(path), child);
      child_path = child_path;
      ops.push_back(zk->build_delete_op(child_path));
      std::vector<std::uint8_t> block_vec;
      std::uint64_t block;
      if (!zk->get(child_path, block_vec, error_code, sizeof(block))) {
        LOG(ERROR) << "[destroy_helper] failed block retrieval "
                "in destroy helper";
        return DeleteResponse::FailedBlockRetrieval;
      }
      block = *reinterpret_cast<std::uint64_t *>(block_vec.data());
      std::vector<std::string> datanodes;

      if (!zk->get_children(get_block_metadata_path(block),
                            datanodes, error_code)) {
        LOG(ERROR) << "[destroy_helper] Failed getting datanode "
                "locations for block: "
                   << block
                   << " with error: "
                   << error_code;
        return DeleteResponse::FailedDataNodeRetrieval;
      }
      // push delete commands onto ops
      for (auto &dn : datanodes) {
        auto delete_queue = util::concat_path(DELETE_QUEUES, dn);
        auto delete_item = util::concat_path(delete_queue, "block-");
        LOG(INFO) << "[destroy_helper] pushed create " << delete_item;
        ops.push_back(zk->build_create_op(delete_item, block_vec,
                                          ZOO_SEQUENCE));
        blockDeleted(block, dn);
      }
    }
    ops.push_back(zk->build_delete_op(ZookeeperBlocksPath(path)));

    // Delete the lease branch
    auto lease_children = std::vector<std::string>();
    if (!zk->get_children(LeaseZookeeperPath(path),
                          lease_children, error_code)) {
      LOG(FATAL) << "[destroy_helper] Failed to get children for "
                 << LeaseZookeeperPath(path);
      return DeleteResponse::FailedChildRetrieval;
    }
    for (auto &child : lease_children) {
      LOG(INFO) << "[destroy_helper] pushed delete " << util::concat_path(
        LeaseZookeeperPath(path), child);
      ops.push_back(zk->build_delete_op(util::concat_path(
        LeaseZookeeperPath(path), child)));
    }
    ops.push_back(zk->build_delete_op(LeaseZookeeperPath(path)));
  }
  ops.push_back(zk->build_delete_op(ZookeeperFilePath(path)));
  return DeleteResponse::Ok;
}

void ZkNnClient::complete(CompleteRequestProto& req,
                          CompleteResponseProto& res,
                          std::string client_name) {
  // TODO(nate):  A call to complete() will not return true until
  // all the file's blocks have been replicated the minimum number of times.
  int error_code;
  // change the under construction bit
  const std::string& src = req.src();
  FileZNode znode_data;
  read_file_znode(znode_data, src);

  // Check access
  if (!checkAccess(client_name, znode_data)) {
    LOG(ERROR) << "[complete] Access denied to path" << src;
    return;
  }

  znode_data.file_status = FileStatus::FileComplete;
  // set the file length
  uint64_t file_length = 0;
  auto file_blocks = std::vector<std::string>();
  if (!zk->get_children(ZookeeperBlocksPath(src), file_blocks, error_code)) {
    LOG(ERROR) << "[complete] Failed getting children of "
               << ZookeeperBlocksPath(src)
               << " with error: "
               << error_code;
    res.set_result(false);
    return;
  }
  if (file_blocks.size() == 0) {
    res.set_result(true);
  }    LOG(ERROR) << "[complete] No blocks found for file "
                  << ZookeeperFilePath(src);

  for (auto file_block : file_blocks) {
    auto data = std::vector<std::uint8_t>();
    if (!zk->get(ZookeeperBlocksPath(src) + "/" + file_block, data,
                 error_code, sizeof(uint64_t))) {
      LOG(ERROR) << "[complete] Failed to get "
                 << ZookeeperBlocksPath(src)
                 << "/"
                 << file_block
                 << " with error: "
                 << error_code;
      res.set_result(false);
      return;
    }
    uint64_t block_uuid = *reinterpret_cast<uint64_t *>(&data[0]);
    auto block_data = std::vector<std::uint8_t>();
    std::string block_metadata_path = get_block_metadata_path(block_uuid);
    if (!zk->get(block_metadata_path,
                 block_data, error_code, sizeof(uint64_t))) {
      LOG(ERROR) << "[complete] Failed to get "
                 << block_metadata_path
                 << " with error: "
                 << error_code;
      res.set_result(false);
      return;
    }

    // TODO(nate): figure out why this value is the length.
    uint64_t length = *reinterpret_cast<uint64_t *>(&block_data[0]);
    file_length += length;
  }
  znode_data.length = file_length;
  std::vector<std::uint8_t> data(sizeof(znode_data));
  file_znode_struct_to_vec(&znode_data, data);
  if (!zk->set(ZookeeperFilePath(src), data, error_code)) {
      LOG(ERROR)
          << "[complete] complete could not change the "
                  "construction bit and file length";
      res.set_result(false);
      return;
  }

  // Remove this client from the leases branch.
  bool exists;
  std::string req_client_name = req.clientname();
  if (req_client_name.size() != 0) {
    if (!zk->exists(LeaseZookeeperPath(src) + '/' +
                      req_client_name, exists, error_code)) {
      LOG(ERROR) << "[complete] Failed to check the existence of "
      << LeaseZookeeperPath(src) + '/' + req_client_name << ".";
      res.set_result(false);
      return;
    }
    if (exists) {
      if (!zk->delete_node(LeaseZookeeperPath(src) + '/'
                           + req_client_name, error_code, true)) {
        LOG(ERROR) << "[complete] Failed to delete node "
        << LeaseZookeeperPath(src)
           + '/' + req_client_name << ".";
        res.set_result(false);
        return;
      }
    }
    // Remove the client from the client branch
    if (!zk->exists(ClientZookeeperPath(req_client_name), exists, error_code)) {
      LOG(ERROR) << "[complete] Failed to check the existence of " <<
        ClientZookeeperPath(req_client_name) << ".";
      res.set_result(false);
      return;
    }
    if (exists) {
      if (!zk->delete_node(ClientZookeeperPath(req_client_name),
                           error_code, true)) {
        LOG(ERROR) << "[complete] Failed to delete node " <<
          ClientZookeeperPath(req_client_name) << ".";
        res.set_result(false);
        return;
      }
    }
  }
  res.set_result(true);
}

/**
 * Go down directories recursively. If a child is a file, then put its deletion on a queue.
 * Files delete themselves, but directories are deleted by their parent (so root can't be deleted)
 */
ZkNnClient::DeleteResponse ZkNnClient::destroy(
    DeleteRequestProto &request,
    DeleteResponseProto &response,
    std::string client_name) {
  int error_code;
  const std::string &path = request.src();
  bool recursive = request.recursive();
  response.set_result(true);

  if (!file_exists(path)) {
    LOG(ERROR) << "[destroy] Cannot delete "
               << path
               << " because it doesn't exist.";
    response.set_result(false);
    return DeleteResponse::FileDoesNotExist;
  }

  FileZNode znode_data;
  read_file_znode(znode_data, path);

  // Check access
  if (!checkAccess(client_name, znode_data)) {
    LOG(ERROR) << "[destroy] Access denied to path " << path;
    return DeleteResponse::FileAccessRestricted;
  }

  if (znode_data.file_type == FileType::File
      && znode_data.file_status == FileStatus::UnderConstruction) {
    LOG(ERROR) << "[destroy] Cannot delete "
               << path
               << " because it is under construction.";
    response.set_result(false);
    return DeleteResponse::FileUnderConstruction;
  }

  if (znode_data.file_type == FileType::Dir && !recursive) {
    LOG(ERROR) << "[destroy] Cannot delete "
               << path
               << " because it is a directory. Use recursive = true.";
    response.set_result(false);
    return DeleteResponse::FileIsDirectoryMismatch;
  }

  std::vector<std::shared_ptr<ZooOp>> ops;
  auto status = destroy_helper(path, ops);
  if (status != DeleteResponse::Ok) {
    response.set_result(false);
    return status;
  }

  LOG(INFO) << "[destroy] Deleting multiop has "
          << ops.size()
          << " operations. Executing...";
  std::vector<zoo_op_result> results;
  if (!zk->execute_multi(ops, results, error_code)) {
    LOG(ERROR) <<
               "[destroy] Failed to execute multi op to delete " <<
                path << " Co: " << error_code;
    response.set_result(false);

    for (int i = 0; i < results.size(); i++) {
      LOG(ERROR) << "[destroy] \t MULTIOP #" << i << " ERROR CODE: "
                 << results[i].err;
    }
    return DeleteResponse::FailedZookeeperOp;
  }

  return DeleteResponse::Ok;
}

/**
 * Create a new file entry in the namespace.
 *
 * This will create an empty file specified by the source path, a full path originated at the root.
 *
 */
ZkNnClient::CreateResponse ZkNnClient::create_file(
        CreateRequestProto &request,
        CreateResponseProto &response) {
  const std::string &path = request.src();
  LOG(INFO) << "[create_file] Trying to create file " << path;
  const std::string &owner = request.clientname();
  bool create_parent = request.createparent();
  std::uint64_t blocksize = request.blocksize();
  std::uint32_t replication = request.replication();
  std::uint32_t createflag = request.createflag();
  const std::string &inputECPolicyName = request.ecpolicyname();

  if (file_exists(path)) {
    LOG(ERROR) << "[create_file] File already exists";
    return CreateResponse::FileAlreadyExists;
  }

  // If we need to create directories, do so
  if (create_parent) {
    LOG(INFO) << "[create_file] Creating directories to store ";
    std::string directory_paths = "";
    std::vector<std::string> split_path;
    boost::split(split_path, path, boost::is_any_of("/"));
    for (int i = 1; i < split_path.size() - 1; i++) {
      directory_paths += ("/" + split_path[i]);
    }
    // try and make all the parents
    if (mkdir_helper(directory_paths, true) !=
        ZkNnClient::MkdirResponse::Ok) {
      LOG(ERROR) << "[create_file] Failed to Mkdir for " << directory_paths;
      return CreateResponse::FailedMkdir;
    }
  }

  // Now create the actual file which will hold blocks
  FileZNode znode_data;
  znode_data.length = 0;
  znode_data.file_status = FileStatus::UnderConstruction;
  uint64_t mslong = current_time_ms();
  znode_data.access_time = mslong;
  znode_data.modification_time = mslong;
  snprintf(znode_data.owner, strlen(znode_data.owner), owner.c_str());
  snprintf(znode_data.group, strlen(znode_data.group), owner.c_str());
  znode_data.replication = replication;
  znode_data.blocksize = blocksize;
  znode_data.file_type = FileType::File;
  // Initialize permissions for file with owner and admin.
  snprintf(znode_data.permissions[0], MAX_USERNAME_LEN, owner.c_str());
  znode_data.perm_length = 1;

  // in the case of EC, this inputECPolicyName is empty.
  if (inputECPolicyName.empty()) {
    znode_data.isEC = false;
  } else {
    znode_data.isEC = true;
  }

  // if we failed, then do not set any status
  if (!create_file_znode(path, &znode_data))
      return CreateResponse::FailedCreateZnode;

  HdfsFileStatusProto *status = response.mutable_fs();
  set_file_info(status, path, znode_data);
  return CreateResponse::Ok;
}

/**
 * Rename a file in the zookeeper filesystem
 */
ZkNnClient::RenameResponse ZkNnClient::rename(RenameRequestProto& req,
                                              RenameResponseProto& res,
                                              std::string client_name) {
  std::string file_path = req.src();

  FileZNode znode_data;
  read_file_znode(znode_data, file_path);

  // Check access
  if (!checkAccess(client_name, znode_data)) {
    LOG(ERROR) << "[rename] Access denied to path " << file_path;
    return RenameResponse::FileAccessRestricted;
  }

  if (!file_exists(file_path)) {
    LOG(ERROR) << "[rename] Requested rename source: "
               << file_path << " does not exist";
    res.set_result(false);
      return RenameResponse::FileDoesNotExist;
  }

  auto ops = std::vector<std::shared_ptr<ZooOp>>();
  if (znode_data.file_type == FileType::Dir) {
    if (!rename_ops_for_dir(req.src(), req.dst(), ops)) {
      LOG(ERROR) << "[rename] Failed to generate rename operatons for: "
                 << file_path;
      res.set_result(false);
        return RenameResponse::RenameOpsFailed;
    }

  } else if (znode_data.file_type == FileType::File) {
    if (!rename_ops_for_file(req.src(), req.dst(), ops)) {
      LOG(ERROR) << "[rename] Failed to generate rename operatons for: "
                 << file_path;
      res.set_result(false);
        return RenameResponse::RenameOpsFailed;
    }

  } else {
    LOG(ERROR) << "[rename] Requested rename source: "
               << file_path
               << " is not a file or dir";
    res.set_result(false);
      return RenameResponse::InvalidType;
  }

  LOG(INFO) << "[rename] Renameing multiop has "
            << ops.size()
            << " operations. Executing...";

  int error_code;
  std::vector<zoo_op_result> results;
  if (!zk->execute_multi(ops, results, error_code)) {
    LOG(ERROR) << "[rename] Failed multiop when renaming: '"
               << req.src()
               << "' to '"
               << req.dst()
               << "'";
    for (int i = 0; i < results.size(); i++) {
      LOG(ERROR) << "[rename] \t MULTIOP #" << i << " ERROR CODE: "
                 << results[i].err;
    }
    res.set_result(false);
  } else {
    LOG(INFO) << "[rename] Successfully exec'd multiop to rename "
              << req.src()
              << " to "
              << req.dst();
    res.set_result(true);
      return RenameResponse::Ok;
  }
}

// ------- make a directory

/**
* Set the default information for a directory znode
*/
void ZkNnClient::set_mkdir_znode(FileZNode *znode_data) {
  znode_data->length = 0;
  uint64_t mslong = current_time_ms();
  znode_data->access_time = mslong;
  znode_data->modification_time = mslong;
  znode_data->blocksize = 0;
  znode_data->replication = 0;
  znode_data->file_type = FileType::Dir;
  znode_data->isEC = false;
  // Note no permissions list because this is a directory not a file.
  znode_data->perm_length = -1;
}

/**
* Make a directory in zookeeper
*/
ZkNnClient::MkdirResponse ZkNnClient::mkdir(MkdirsRequestProto &request,
                                            MkdirsResponseProto &response) {
  const std::string &path = request.src();
  bool create_parent = request.createparent();
  auto rv = mkdir_helper(path, create_parent);
  response.set_result(rv == MkdirResponse::Ok);
  return rv;
}

/**
* Helper for creating a directory znode. Iterates over the parents and creates
* them if necessary.
*/
ZkNnClient::MkdirResponse ZkNnClient::mkdir_helper(const std::string &path,
                           bool create_parent) {
  LOG(INFO) << "[mkdir_helper] mkdir_helper called with input " << path;
  if (create_parent) {
    std::vector<std::string> split_path;
    boost::split(split_path, path, boost::is_any_of("/"));
    bool not_exist = false;
    std::string unroll;
    std::string p_path;
    // Start at index 1 because it includes "/" as the first element
    // in the array when we do NOT want that
    for (int i = 1; i < split_path.size(); i++) {
      p_path += "/" + split_path[i];
      LOG(INFO) << "[mkdir_helper] " << p_path;
      if (!file_exists(p_path)) {
        // keep track of the path where we start creating directories
        if (!not_exist) {
          unroll = p_path;
        }
        not_exist = true;
        FileZNode znode_data;
        set_mkdir_znode(&znode_data);
        if (!create_file_znode(p_path, &znode_data)) {
          // TODO(2016) unroll the created directories
          return MkdirResponse::FailedZnodeCreation;
        }
      }
    }
  } else {
    FileZNode znode_data;
    set_mkdir_znode(&znode_data);
    return create_file_znode(path, &znode_data) ?
         MkdirResponse::Ok : MkdirResponse::FailedZnodeCreation;
  }
  return MkdirResponse::Ok;
}

ZkNnClient::ListingResponse ZkNnClient::get_listing(
    GetListingRequestProto &req,
    GetListingResponseProto &res,
    std::string client_name) {
  int error_code;

  const std::string &src = req.src();

  if (cache->contains(src)) {
    // Get cached
        LOG(INFO) << "[get_listing] Found path " << src << " listing in cache";
    auto listing = cache->get(src);
    auto l_copy = new DirectoryListingProto(*listing.get()->mutable_dirlist());
    res.set_allocated_dirlist(l_copy);
  } else {
        LOG(INFO) << "[get_listing] Did not find path " << src
                  << " listing in cache";
    // From 2016:
    // if src is a file then just return that file with remaining = 0
    // otherwise return first 1000 files in src dir starting at start_after
    // and set remaining to the number left after that first 1000

    bool exists;
    int error_code;

    const int start_after = req.startafter();
    const bool need_location = req.needlocation();
    DirectoryListingProto *raw_listing = res.mutable_dirlist();

    // Set watcher on root and examine node
    if (zk->wexists(ZookeeperFilePath(src),
                    exists,
                    watcher_listing,
                    this,
                    error_code) && exists) {
      // Read node data
      FileZNode znode_data;
      read_file_znode(znode_data, src);

      // Check access
      if (!checkAccess(client_name, znode_data)) {
        LOG(ERROR) << "[get_listing] Access denied to path " << src;
        return ListingResponse::FileAccessRestricted;
      }

      if (znode_data.file_type == FileType::File) {
        // Update listing with file info
        HdfsFileStatusProto *status = raw_listing->add_partiallisting();
        set_file_info(status, src, znode_data);
        if (need_location) {
          LocatedBlocksProto *blocks = status->mutable_locations();
          get_block_locations(src, 0, znode_data.length, blocks);
        }
      } else {
        // Update listing with directory info
        std::vector<std::string> children;
        if (!zk->wget_children(ZookeeperFilePath(src),
                               children,
                               watcher_listing,
                               this,
                               error_code)) {
          LOG(FATAL) << "[get_listing] Failed to get children for "
                     << ZookeeperFilePath(src);
          return ListingResponse::FailedChildRetrieval;
        } else {
          for (auto &child : children) {
            auto child_path = util::concat_path(src, child);

            FileZNode child_data;
            read_file_znode(child_data, child_path);

            // Check access
            if (!checkAccess(client_name, child_data)) {
              LOG(ERROR) << "[get_listing] Access denied to path "
                         << child_path;
              return ListingResponse::FileAccessRestricted;
            }

            HdfsFileStatusProto *status = raw_listing->add_partiallisting();
            set_file_info(status, child_path, child_data);

            if (need_location) {
              LocatedBlocksProto *blocks = status->mutable_locations();
              get_block_locations(child_path, 0, child_data.length, blocks);
            }
          }
        }
      }
      raw_listing->set_remainingentries(0);
      auto listing = std::make_shared<GetListingResponseProto> (res);
      cache->insert(src, listing);
      LOG(INFO) << "[get_listing] Adding path to cache " << src;
    } else {
      LOG(ERROR) << "[get_listing] File does not exist with name " << src;
      return ListingResponse::FileDoesNotExist;
    }
  }
  return ListingResponse::Ok;
}

void ZkNnClient::get_block_locations(GetBlockLocationsRequestProto &req,
                                     GetBlockLocationsResponseProto &res,
                                     std::string client_name) {
  const std::string &src = req.src();
  google::protobuf::uint64 offset = req.offset();
  google::protobuf::uint64 length = req.length();
  LocatedBlocksProto *blocks = res.mutable_locations();
  get_block_locations(src, offset, length, blocks, client_name);
}

void ZkNnClient::get_block_locations(const std::string &src,
                                     google::protobuf::uint64 offset,
                                     google::protobuf::uint64 length,
                                     LocatedBlocksProto *blocks,
                                     std::string client_name) {
  int error_code;
  const std::string zk_path = ZookeeperBlocksPath(src);

  FileZNode znode_data;
  read_file_znode(znode_data, src);

  // Check access
  if (!checkAccess(client_name, znode_data)) {
    LOG(ERROR) << "[get_block_locations] Access denied to path " << src;
    return;
  }

  blocks->set_underconstruction(false);
  blocks->set_islastblockcomplete(true);
  blocks->set_filelength(znode_data.length);

<<<<<<< HEAD
  uint64_t block_size;
=======
  uint64_t block_size = znode_data.blocksize;

  LOG(INFO) << "[get_block_locations] Block size of " << zk_path << " is "
            << block_size;
>>>>>>> ZkNnClient log cleanup (#190)

  auto sorted_blocks = std::vector<std::string>();

  // TODO(2016): Make more efficient
  if (!zk->get_children(zk_path, sorted_blocks, error_code)) {
    LOG(ERROR) << "[get_block_locations] Failed getting children of "
               << zk_path
               << " with error: "
               << error_code;
  }

  std::sort(sorted_blocks.begin(), sorted_blocks.end());

  uint64_t size = 0;
  for (auto sorted_block : sorted_blocks) {
    LOG(INFO) << "[get_block_locations] Considering block " << sorted_block;
    if (size > offset + length) {
      // at this point the start of the block is at a higher
      // offset than the segment we want
      LOG(INFO) << "[get_block_locations] Breaking at block " << sorted_block;
      break;
    }
<<<<<<< HEAD
=======
    if (size + block_size >= offset) {
      auto data = std::vector<uint8_t>();
      if (!zk->get(zk_path + "/" + sorted_block, data,
                   error_code, sizeof(uint64_t))) {
        LOG(ERROR) << "[get_block_locations] Failed to get "
                   << zk_path << "/"
                   << sorted_block
                   << " info: "
                   << error_code;
        return;  // TODO(2016): Signal error
      }
      uint64_t block_id = *reinterpret_cast<uint64_t *>(&data[0]);
      LOG(INFO) << "[get_block_locations] Found block " << block_id
                << " for " << zk_path;
>>>>>>> ZkNnClient log cleanup (#190)

    // Retreive block size
    auto data = std::vector<uint8_t>();
    if (!zk->get(zk_path + "/" + sorted_block, data,
                 error_code, sizeof(uint64_t))) {
      LOG(ERROR) << "[get_block_locations] Failed to get "
      << zk_path << "/"
      << sorted_block
      << " info: "
      << error_code;
      return;  // TODO(2016): Signal error
    }
    uint64_t block_id = *reinterpret_cast<uint64_t *>(&data[0]);
    LOG(INFO) << "[get_block_locations] Found block " << block_id
    << " for " << zk_path;
    std::string block_metadata_path = get_block_metadata_path(block_id);
    BlockZNode block_data;
    std::vector<std::uint8_t> block_data_vec(sizeof(block_data));
    if (!zk->get(block_metadata_path, block_data_vec, error_code,
                 sizeof(block_data))) {
      LOG(ERROR) << "[get_block_locations] Failed getting block size for "
      << block_metadata_path << " with error " << error_code;
    }
    memcpy(&block_data, &block_data_vec[0], sizeof(block_data));
    block_size = block_data.block_size;
    LOG(INFO) << "[get_block_locations] Block size of " << zk_path << "block "
    << block_id << " is " << block_size;

    if (size + block_size >= offset) {
      // TODO(2016): This block of code should be moved to a function,
      // repeated with add_block
      LocatedBlockProto *located_block = blocks->add_blocks();
      located_block->set_corrupt(0);
      // TODO(2016): This offset may be incorrect
      located_block->set_offset(size);

      buildExtendedBlockProto(located_block->mutable_b(), block_id, block_size);

      auto data_nodes = std::vector<std::string>();
<<<<<<< HEAD
=======
      std::string block_metadata_path = get_block_metadata_path(block_id);
>>>>>>> ZkNnClient log cleanup (#190)
      LOG(INFO) << "[get_block_locations] Getting datanode locations for block:"
                << block_metadata_path;

      if (!zk->get_children(block_metadata_path,
                            data_nodes, error_code)) {
        LOG(ERROR) << "[get_block_locations] Failed getting datanode "
                "locations for block: "
                   << block_metadata_path
                   << " with error: "
                   << error_code;
      }

      auto sorted_data_nodes = std::vector<std::string>();
      if (sort_by_xmits(data_nodes, sorted_data_nodes)) {
        for (auto data_node = sorted_data_nodes.begin();
             data_node != sorted_data_nodes.end();
             ++data_node) {
          LOG(INFO) << "[get_block_locations] Block DN Loc: " << *data_node;
          buildDatanodeInfoProto(located_block->add_locs(), *data_node);
        }
      } else {
        LOG(ERROR)
            << "[get_block_locations] Unable to sort DNs by # xmits in "
                    "get_block_locations. Using unsorted instead.";
        for (auto data_node = data_nodes.begin();
             data_node != data_nodes.end();
             ++data_node) {
          LOG(INFO) << "[get_block_locations] Block DN Loc: " << *data_node;
          buildDatanodeInfoProto(located_block->add_locs(), *data_node);
        }
      }

      buildTokenProto(located_block->mutable_blocktoken());
    }
    size += block_size;
  }
}


ZkNnClient::ErasureCodingPoliciesResponse
ZkNnClient::get_erasure_coding_policies(
    GetErasureCodingPoliciesRequestProto &req,
    GetErasureCodingPoliciesResponseProto &res) {

  auto ec_policies = res.mutable_ecpolicies();
  ErasureCodingPolicyProto *ec_policy_to_add = ec_policies->Add();
  // TODO(nate): will have to change if we support multiple EC policies.
  ec_policy_to_add->set_id(DEFAULT_EC_ID);
  ec_policy_to_add->set_name(DEFAULT_EC_POLICY);
  ec_policy_to_add->set_cellsize(DEFAULT_EC_CELLCIZE);
  ECSchemaProto* ecSchemaProto = ec_policy_to_add->mutable_schema();
  ecSchemaProto->set_codecname(DEFAULT_EC_CODEC_NAME);
  ecSchemaProto->set_dataunits(DEFAULT_DATA_UNITS);
  ecSchemaProto->set_parityunits(DEFAULT_PARITY_UNITS);

  return ErasureCodingPoliciesResponse::Ok;
}

ZkNnClient::ErasureCodingPolicyResponse
ZkNnClient::get_erasure_coding_policy_of_path(
    GetErasureCodingPolicyRequestProto &req,
    GetErasureCodingPolicyResponseProto &res) {
  std::string file_src = req.src();
  if (!file_exists(file_src)) {
    return ErasureCodingPolicyResponse::FileDoesNotExist;
  }

  FileZNode znode_data;
  read_file_znode(znode_data, file_src);

  // Set the EC policy proto.
  // In the case of Replication, the client expects null.
  // In the case of EC, populate it with values.

  if (znode_data.isEC) {
    // TODO(nate): will have to change if we support multiple EC policies.
    ErasureCodingPolicyProto* ecPolicyProto = res.mutable_ecpolicy();
    ecPolicyProto->set_id(DEFAULT_EC_ID);
    ecPolicyProto->set_name(DEFAULT_EC_POLICY);
    ecPolicyProto->set_cellsize(DEFAULT_EC_CELLCIZE);
    ECSchemaProto* ecSchemaProto = ecPolicyProto->mutable_schema();
    ecSchemaProto->set_codecname(DEFAULT_EC_CODEC_NAME);
    ecSchemaProto->set_dataunits(DEFAULT_DATA_UNITS);
    ecSchemaProto->set_parityunits(DEFAULT_PARITY_UNITS);
  }

  return ErasureCodingPolicyResponse::Ok;
}

ZkNnClient::SetErasureCodingPolicyResponse
ZkNnClient::set_erasure_coding_policy_of_path(
    SetErasureCodingPolicyRequestProto &req,
    SetErasureCodingPolicyResponseProto &res) {

  std::string file_src = req.src();
  std::string ecpolicy_name = req.ecpolicyname();

  if (!file_exists(file_src)) {
    return SetErasureCodingPolicyResponse::FileDoesNotExist;
  }

  FileZNode znode_data;
  read_file_znode(znode_data, file_src);

  // TODO(nate): this boolean flag must change if
  // we support multiple ec policies.
  if (ecpolicy_name.empty()) {
    znode_data.isEC = 0;
  } else {
    znode_data.isEC = 1;
  }
  int error_code;
  std::vector<std::uint8_t> data(sizeof(znode_data));
  file_znode_struct_to_vec(&znode_data, data);
  if (!zk->set(ZookeeperFilePath(file_src), data, error_code)) {
    LOG(ERROR)
        << "[set_erasure_coding_policy_of_path] complete could not "
                "change the construction bit and file length";
    return SetErasureCodingPolicyResponse::FailedZookeeperOp;
  }
  return SetErasureCodingPolicyResponse::Ok;
}

// ------------------------------ HELPERS ---------------------------

bool ZkNnClient::sort_by_xmits(const std::vector<std::string> &unsorted_dn_ids,
                 std::vector<std::string> &sorted_dn_ids) {
  int error_code;
  std::priority_queue<TargetDN> targets;

  for (auto datanode : unsorted_dn_ids) {
    std::string dn_stats_path = HEALTH_BACKSLASH + datanode + STATS;
    std::vector<uint8_t> stats_payload;
    stats_payload.resize(sizeof(DataNodePayload));
    if (!zk->get(dn_stats_path, stats_payload,
           error_code, sizeof(DataNodePayload))) {
      LOG(ERROR) << "[sort_by_xmits] Failed to get " << dn_stats_path;
      return false;
    }
    DataNodePayload stats = DataNodePayload();
    memcpy(&stats, &stats_payload[0], sizeof(DataNodePayload));
    targets.push(TargetDN(datanode, stats.free_bytes, stats.xmits, MIN_XMITS));
  }

  while (targets.size() > 0) {
    TargetDN target = targets.top();
    sorted_dn_ids.push_back(target.dn_id);
    targets.pop();
  }

  return true;
}
std::string ZkNnClient::ClientZookeeperPath(const std::string & clientname) {
  return std::string(CLIENTS) + '/' + clientname;
}
std::string ZkNnClient::LeaseZookeeperPath(const std::string & hadoopPath) {
  std::string zkpath = ZookeeperFilePath(hadoopPath);
  zkpath += LEASES;
  return zkpath;
}

std::string ZkNnClient::ZookeeperFilePath(const std::string &hadoopPath) {
  std::string zkpath = NAMESPACE_PATH;
  if (hadoopPath.size() == 0) {
    LOG(ERROR) << " this hadoop path is invalid";
  }
  if (hadoopPath.at(0) != '/') {
    zkpath += "/";
  }
  zkpath += hadoopPath;
  if (zkpath.at(zkpath.length() - 1) == '/') {
    zkpath.pop_back();
  }
  return zkpath;
}

std::string ZkNnClient::ZookeeperBlocksPath(const std::string &hadoopPath) {
  std::string zkpath = ZookeeperFilePath(hadoopPath);
  zkpath += BLOCKS_TREE;
  return zkpath;
}

void ZkNnClient::get_content(GetContentSummaryRequestProto &req,
                             GetContentSummaryResponseProto &res,
                             std::string client_name) {
  const std::string &path = req.path();

  if (file_exists(path)) {
    // read the node into the file node struct
    FileZNode znode_data;
    read_file_znode(znode_data, path);

    // Check access
    if (!checkAccess(client_name, znode_data)) {
      LOG(ERROR) << "[get_content] Access denied to path " << path;
      return;
    }

    // set the file status in the get file info response res
    ContentSummaryProto *status = res.mutable_summary();

    set_file_info_content(status, path, znode_data);
    LOG(INFO) << "[get_content] Got info for file " << path;
    return;
  }
  LOG(INFO) << "[get_content] No file to get info for " << path;
  return;
}

void ZkNnClient::set_file_info_content(ContentSummaryProto *status,
                                       const std::string &path,
                                       FileZNode &znode_data) {
  // get the filetype, since we do not want to serialize an enum
  int error_code = 0;
  if (znode_data.file_type == FileType::Dir) {
    int num_file = 0;
    int num_dir = 0;
    std::vector<std::string> children;
    if (!zk->get_children(ZookeeperBlocksPath(path), children, error_code)) {
      LOG(FATAL) << "[set_file_info_content] Failed to get children for "
                 << ZookeeperBlocksPath(path);
    } else {
      for (auto &child : children) {
        auto child_path = util::concat_path(path, child);
        FileZNode child_data;
        read_file_znode(child_data, child_path);
        if (child_data.file_type == FileType::File) {
          num_file += 1;
        } else {
          num_dir += 1;
        }
      }
    }

    status->set_filecount(num_file);
    status->set_directorycount(num_dir);
  } else {
    status->set_filecount(1);
    status->set_directorycount(0);
  }


  // status->set_filetype(filetype);
  // status->set_path(path);
  status->set_length(znode_data.length);
  status->set_quota(1);
  status->set_spaceconsumed(1);
  status->set_spacequota(100000);

  LOG(INFO) << "[set_file_info_content] Successfully set the file info for "
            << path;
}

void ZkNnClient::set_file_info(HdfsFileStatusProto *status,
                               const std::string &path, FileZNode &znode_data) {
  HdfsFileStatusProto_FileType filetype;
  // get the filetype, since we do not want to serialize an enum
  switch (znode_data.file_type) {
    case (FileType::Dir):
      filetype = HdfsFileStatusProto::IS_DIR;
      break;
    case (FileType::File):
      filetype = HdfsFileStatusProto::IS_FILE;
      break;
    default:break;
  }

  FsPermissionProto *permission = status->mutable_permission();
  // Shorcut to set permission to 777.
  // TODO(heliumj): Should this be changed to read permission only for non-owner
  // users?
  permission->set_perm(~0);
  status->set_filetype(filetype);
  status->set_path(path);
  status->set_length(znode_data.length);
  status->set_blocksize(znode_data.blocksize);
  std::string owner(znode_data.owner);
  std::string group(znode_data.group);
  status->set_owner(owner);
  status->set_group(group);

  status->set_modification_time(znode_data.modification_time);
  status->set_access_time(znode_data.access_time);

  // If a block is an EC block, optionally set the ecPolicy field.
  if (znode_data.isEC) {
      LOG(ERROR) << "[set_file_info] Setting EC related proto fields";
      ErasureCodingPolicyProto *ecPolicyProto = status->mutable_ecpolicy();
      ecPolicyProto->set_name(DEFAULT_EC_POLICY);
      ecPolicyProto->set_cellsize(DEFAULT_EC_CELLCIZE);
      ecPolicyProto->set_id(DEFAULT_EC_ID);
      ECSchemaProto* ecSchema = ecPolicyProto->mutable_schema();
      ecSchema->set_codecname(DEFAULT_EC_CODEC_NAME);
      ecSchema->set_dataunits(DEFAULT_DATA_UNITS);
      ecSchema->set_parityunits(DEFAULT_PARITY_UNITS);
  }

  LOG(INFO) << "[set_file_info] Successfully set the file info ";
}
  bool ZkNnClient::set_permission(SetPermissionRequestProto &req,
                                  SetPermissionResponseProto &res) {
    int zk_error;
    const std::string path = req.src();
    FileZNode znode_data;
    if (!file_exists(path)) {
      LOG(ERROR) << "[set_permission] Requested path " << path
                 << " does not exist";
      return false;
    }
    read_file_znode(znode_data, path);
    znode_data.permission_number = req.kPermissionFieldNumber;

    // Serialize struct to byte vector
    std::vector<std::uint8_t> zk_data(sizeof(FileZNode));
    file_znode_struct_to_vec(&znode_data, zk_data);

    // Write the modified node back to Zookeeper
    zk->set(ZookeeperFilePath(path), zk_data, zk_error);

    if (zk_error != ZK_ERRORS::OK) {
      LOG(ERROR) << "[set_permission] ZK reported error writing modified "
              "node back to disk";
      return false;
    }

    return true;
  }

bool ZkNnClient::add_block(const std::string &file_path,
               std::uint64_t &block_id,
               std::vector<std::string> &data_nodes,
               uint32_t replicationFactor) {
  if (!file_exists(file_path)) {
    LOG(ERROR) << "[add_block] Cannot add block to non-existent file"
               << file_path;
    return false;
  }

  FileZNode znode_data;
  read_file_znode(znode_data, file_path);
  // TODO(2016): This is a faulty check
  if (znode_data.file_status == FileStatus::UnderConstruction) {
    LOG(WARNING) << "[add_block] Last block for "
           << file_path
           << " still under construction";
  }
  // TODO(2016): Check the replication factor

  std::string block_id_str;

  util::generate_block_id(block_id);
  block_id_str = std::to_string(block_id);
  LOG(INFO) << "[add_block] Generated block id " << block_id_str;
  auto excluded_dns = std::vector<std::string>();
  if (!find_datanode_for_block(data_nodes, excluded_dns, block_id,
       replicationFactor, znode_data.blocksize)) {
    return false;
  }

  // Generate the massive multi-op for creating the block

  std::vector<std::uint8_t> data;
  data.resize(sizeof(u_int64_t));
  memcpy(&data[0], &block_id, sizeof(u_int64_t));

  LOG(INFO) << "[add_block] Generating block for "
            << ZookeeperBlocksPath(file_path);

  // ZooKeeper multi-op to add block
  // (Note the actual path of the znode the first create op makes will have a 9
  // digit number appended onto the end because of the ZOO_SEQUENCE flag)
  auto seq_file_block_op = zk->build_create_op(
      ZookeeperBlocksPath(file_path) + "/block_",
      data,
      ZOO_SEQUENCE);
  auto ack_op = zk->build_create_op(
      "/work_queues/wait_for_acks/" + block_id_str,
      ZKWrapper::EMPTY_VECTOR);
  auto block_location_op = zk->build_create_op(
      get_block_metadata_path(block_id),
      ZKWrapper::EMPTY_VECTOR);

  std::vector<std::shared_ptr<ZooOp>> ops = {seq_file_block_op,
                         ack_op,
                         block_location_op};

  auto results = std::vector<zoo_op_result>();
  int err;
  // We do not need to sync this multi-op immediately
  if (!zk->execute_multi(ops, results, err, false)) {
    LOG(ERROR)
        << "[add_block] Failed to write the addBlock multiop, "
                "ZK state was not changed";
    ZKWrapper::print_error(err);
    return false;
  }
  return true;
}

bool ZkNnClient::add_block_group(const std::string &filePath,
                     u_int64_t &block_group_id,
                     std::vector<std::string> &dataNodes,
                     std::vector<char> &blockIndices,
                     uint32_t total_num_storage_blocks) {
  FileZNode znode_data;
  read_file_znode(znode_data, filePath);
  std::vector<u_int64_t> storage_block_ids;
  // Generate the block group id and storage block ids.
  block_group_id = generate_block_group_id();
  char blockIndex = 0;
  for (u_int64_t i = 0; i < total_num_storage_blocks; i++) {
    storage_block_ids.push_back(generate_storage_block_id(
        block_group_id, i));
    blockIndices.push_back(blockIndex);
    blockIndex++;
  }

  // Log them for debugging purposes.
  LOG(INFO) << "[add_block_group] Generated block group id "
            << block_group_id << "\n";
  for (auto storageBlockID : storage_block_ids)
      LOG(INFO) << "[add_block_group] Generated storage block id "
                << storageBlockID << "\n";

  // TODO(nate): find_data_node_for_block may have some other logic
  // baked into it. also, repeatedly calling it may return the same
  // data node id.
  std::vector<std::string> tempDataNodes;
  for (int i = 0; i < total_num_storage_blocks; i++) {
    find_datanode_for_block(
        tempDataNodes, dataNodes,
        storage_block_ids[i], 1, znode_data.blocksize);
    dataNodes.push_back(tempDataNodes[0]);
    tempDataNodes.clear();
  }

  std::vector<std::shared_ptr<ZooOp>> ops;
  auto block_location_op = zk->build_create_op(
      get_block_metadata_path(block_group_id),
      ZKWrapper::EMPTY_VECTOR);
  ops.push_back(block_location_op);
  LOG(INFO)
      << "[add_block_group] Added the ZK operation that creates "
              "the block_group_id"
      << std::endl;


  for (auto storageBlockID : storage_block_ids) {
    auto storage_block_op = zk->build_create_op(
        get_block_metadata_path(
            storageBlockID) + "/" + std::to_string(storageBlockID),
        ZKWrapper::EMPTY_VECTOR);
    ops.push_back(storage_block_op);
    LOG(INFO)
        << "[add_block_group] Added the ZK operation that creates "
                "the storage_block"
        << " " << storageBlockID
        << std::endl;
  }

  auto results = std::vector<zoo_op_result>();
  int err;

  if (!zk->execute_multi(ops, results, err, false)) {
    LOG(ERROR)
      << "[add_block_group] Failed to write the addBlock multiop, "
              "ZK state was not changed"
      << std::endl;
    ZKWrapper::print_error(err);

    return false;
  }
  return true;
}


u_int64_t ZkNnClient::generate_storage_block_id(
        u_int64_t block_group_id,
        u_int64_t index_within_group) {
    // TODO(nate): assume a file = no more than 2**47 128 MB blocks
    // TODO(nate): assume no more than 2**16 storage blocks in a logical block
    u_int64_t res = block_group_id;  // filled with 1 and 63 zeros.
    res = res | index_within_group;  // & to fill the lower 16 bits.
    return res;
}

u_int64_t ZkNnClient::generate_block_group_id() {
    u_int64_t res;
    util::generate_block_id(res);  // generate some random 64 bits.
    res = res | (1ull << 63);  // filp the highest bit to one.
    res = (res >> 16) << 16;  // fill the lower 16 bits with zeros.
    return res;
}

u_int64_t ZkNnClient::get_block_group_id(u_int64_t storage_block_id) {
    u_int64_t mask = (~(0ull) << 16);
    return storage_block_id & mask;
}

u_int64_t ZkNnClient::get_index_within_block_group(u_int64_t storage_block_id) {
    u_int64_t mask = 0xffff;  // 48 zeroes and 16 ones.
    return storage_block_id & mask;
}


bool ZkNnClient::find_live_datanodes(const uint64_t blockId, int error_code,
                                    std::vector<std::string> &live_data_nodes) {
  std::string block_metadata_path = get_block_metadata_path(blockId);
  return zk->get_children(HEALTH, live_data_nodes, error_code);
}


bool ZkNnClient::find_datanode_for_block(std::vector<std::string> &datanodes,
                                        std::vector<std::string> &excluded_dns,
                                         const u_int64_t blockId,
                                         uint32_t replication_factor,
                                         uint64_t blocksize) {
  // TODO(2016): Actually perform this action
  // TODO(2016): Perhaps we should keep a cached list of nodes

  std::vector<std::string> live_data_nodes = std::vector<std::string>();
  int error_code;
  // Get all of the live datanodes
  if (find_live_datanodes(blockId, error_code, live_data_nodes)) {
    std::priority_queue<TargetDN> targets;
    /* for each child, check if the ephemeral node exists */
    for (auto datanode : live_data_nodes) {
      bool isAlive;
      if (!zk->exists(HEALTH_BACKSLASH + datanode + HEARTBEAT,
              isAlive, error_code)) {
        LOG(ERROR) << "[find_datanode_for_block] Failed to "
                              "check if datanode: " + datanode
               << " is alive: "
               << error_code;
      }
      if (isAlive) {
        bool exclude = false;
        if (excluded_dns.size() > 0) {
          // Remove the excluded datanodes from the live list
          std::vector<std::string>::iterator it = std::find(
              excluded_dns.begin(),
              excluded_dns.end(),
              datanode);

          if (it == excluded_dns.end()) {
            // This datanode was not in the excluded list, so keep it
            exclude = false;
          } else {
            exclude = true;
          }
        }

        if (!exclude) {
          std::string dn_stats_path = HEALTH_BACKSLASH + datanode + STATS;
          std::vector<uint8_t> stats_payload;
          stats_payload.resize(sizeof(DataNodePayload));
          if (!zk->get(dn_stats_path, stats_payload,
                 error_code, sizeof(DataNodePayload))) {
            LOG(ERROR) << "[find_datanode_for_block] Failed to get "
                       << dn_stats_path;
            return false;
          }
          DataNodePayload stats = DataNodePayload();
          memcpy(&stats, &stats_payload[0], sizeof(DataNodePayload));
          LOG(INFO) << "[find_datanode_for_block] \t DN stats - free_bytes: "
                << unsigned(stats.free_bytes);
          if (stats.free_bytes > blocksize) {
            auto queue = REPLICATE_QUEUES + datanode;
            auto repl_item = util::concat_path(queue, std::to_string(blockId));
            bool alreadyOnQueue;
            // do not put something on queue if its already on there
            if (zk->exists(repl_item, alreadyOnQueue, error_code)) {
              if (alreadyOnQueue) {
                LOG(INFO) << "[find_datanode_for_block] Skipping target"
                          << datanode;
                continue;
              }
            }
            LOG(INFO) << "[find_datanode_for_block] Pushed target: "
                  << datanode
                  << " with "
                  << stats.xmits
                  << " xmits";
            targets.push(TargetDN(datanode, stats.free_bytes, stats.xmits,
                        ZkNnClient::policy));
          }
        }
      }
    }

    LOG(INFO) << "[find_datanode_for_block] There are "
          << targets.size()
          << " viable DNs. Requested: "
          << replication_factor;
    if (targets.size() < replication_factor) {
      LOG(ERROR) << "[find_datanode_for_block] Not enough"
              " available DNs! Available: "
             << targets.size()
             << " Requested: "
             << replication_factor;
      // no return because we still want the client to write to some datanodes
    }

    while (datanodes.size() < replication_factor && targets.size() > 0) {
      LOG(INFO) << "[find_datanode_for_block] DNs size IS : "
                << datanodes.size();
      TargetDN target = targets.top();
      datanodes.push_back(target.dn_id);
      LOG(INFO) << "[find_datanode_for_block] Selecting target DN "
            << target.dn_id
            << " with "
            << target.num_xmits
            << " xmits and "
            << target.free_bytes
            << " free bytes";
      targets.pop();
    }

  } else {
    LOG(ERROR) << "[find_datanode_for_block] Failed to get list of datanodes at"
           << std::string(HEALTH) + " "
           << error_code;
    return false;
  }

  // TODO(2016): Read strategy from config,
  // but as of now select the first few blocks that are valid

  /* Select a random subset of the datanodes
   * if we've gotten more than the # requested.
   */
  unsigned int seed = time(NULL);
  while (datanodes.size() > replication_factor) {
    auto rem = rand_r(&seed) % datanodes.size();
    std::swap(datanodes[rem], datanodes.back());
    datanodes.pop_back();
  }

  if (datanodes.size() < replication_factor) {
    LOG(ERROR) << "[find_datanode_for_block] Failed to find at least "
           << replication_factor
           << " datanodes at " + std::string(HEALTH);
    // no return because we still want the client to write to some datanodes
  }

  return true;
}

/**
* Generates multiop ops for renaming src to dst
* @param src The path to the source file (not znode) within the filesystem
* @param dst The path to the renamed destination file (not znode) within the filesystem
* @param ops The vector of multiops which will make up the overall atomic rename operation
* @return Boolean indicating success or failure of the rename
*/
bool ZkNnClient::rename_ops_for_file(const std::string &src,
                                     const std::string &dst,
                                     std::vector<std::shared_ptr<ZooOp>> &ops) {
  int error_code = 0;
  auto data = std::vector<std::uint8_t>();
  std::string src_znode = ZookeeperFilePath(src);
  std::string src_znode_blocks = ZookeeperBlocksPath(src);
  std::string src_znode_leases = LeaseZookeeperPath(src);
  std::string dst_znode = ZookeeperFilePath(dst);
  std::string dst_znode_blocks = ZookeeperBlocksPath(dst);
  std::string dst_znode_leases = LeaseZookeeperPath(dst);

  // Get the payload from the old filesystem znode for the src
  zk->get(src_znode, data, error_code);
  if (error_code != ZOK) {
    LOG(ERROR) << "[rename_ops_for_file] Failed to get data from '"
               << src_znode
               << "' when renaming.";
    return false;
  }

  // Create a new znode in the filesystem for the dst
  LOG(INFO) << "[rename_ops_for_file] Added op#" << ops.size()
            << ": create " << dst_znode;
  ops.push_back(zk->build_create_op(dst_znode, data));

    zk->get(src_znode_blocks, data, error_code);
    if (error_code != ZOK) {
        LOG(ERROR) << "[rename_ops_for_file] Failed to get data from '"
                   << src_znode
                   << "' when renaming.";
        return false;
    }

    // Create a new znode in the filesystem for the dst
    LOG(INFO) << "[rename_ops_for_file] Added op#" << ops.size()
              << ": create " << dst_znode_blocks;
    ops.push_back(zk->build_create_op(dst_znode_blocks, data));

  // Copy over the lease branch
  auto lease_data = std::vector<std::uint8_t>();
  zk->get(src_znode_leases, lease_data, error_code);
  if (error_code != ZOK) {
    LOG(ERROR) << "[rename_ops_for_file] Failed to get data from '"
    << src_znode_leases
    << "' when renaming.";
    return false;
  }
  ops.push_back(zk->build_create_op(dst_znode_leases, lease_data));

  auto lease_children = std::vector<std::string>();
  zk->get_children(src_znode_leases, lease_children, error_code);
  if (error_code != ZOK) {
    LOG(ERROR) << "[rename_ops_for_file] Failed to get children of znode '"
    << src_znode_leases
    << "' when renaming.";
    return false;
  }
  for (auto child : lease_children) {
    auto child_data = std::vector<std::uint8_t>();

    zk->get(src_znode_leases + "/" + child, child_data, error_code);
    if (error_code != ZOK) {
      LOG(ERROR) << "[rename_ops_for_file] Failed to get data from '"
                 << child << "' when renaming.";
      return false;
    }

    // Create new child of dst_znode with this data
    LOG(INFO) << "[rename_ops_for_file] Added op#"
    << ops.size()
    << ": create "
    << dst_znode_leases + "/" + child;
    ops.push_back(zk->build_create_op(dst_znode_leases + "/" + child,
                                      child_data));

    // Delete src_znode's child
    LOG(INFO) << "[rename_ops_for_file] Added op#"
    << ops.size()
    << ": delete "
    << src_znode_leases + "/" + child;
    ops.push_back(zk->build_delete_op(src_znode_leases + "/" + child));
  }

  // Copy over the data from the children of the src_znode
  // into new children of the dst_znode
  auto children = std::vector<std::string>();
  zk->get_children(src_znode_blocks, children, error_code);
  if (error_code != ZOK) {
    LOG(ERROR) << "[rename_ops_for_file] Failed to get children of znode '"
               << src_znode
               << "' when renaming.";
    return false;
  }

  for (auto child : children) {
    // Get child's data
    auto child_data = std::vector<std::uint8_t>();
    zk->get(src_znode_blocks + "/" + child, child_data, error_code);
    if (error_code != ZOK) {
      LOG(ERROR) << "Failed to get data from '" << child << "' when renaming.";
      return false;
    }

    // Create new child of dst_znode with this data
    LOG(INFO) << "[rename_ops_for_file] Added op#"
              << ops.size()
              << ": create "
              << dst_znode + "/" + child;
    ops.push_back(zk->build_create_op(dst_znode_blocks + "/" + child,
                                      child_data));

    // Delete src_znode's child
    LOG(INFO) << "[rename_ops_for_file] Added op#"
              << ops.size()
              << ": delete "
              << src_znode + "/" + child;
    ops.push_back(zk->build_delete_op(src_znode_blocks + "/" + child));
  }

  // Remove the old znode for the src
  ops.push_back(zk->build_delete_op(src_znode_leases));
  LOG(INFO) << "[rename_ops_for_file] Added op#" << ops.size()
            << ": delete " << src_znode_blocks;
  ops.push_back(zk->build_delete_op(src_znode_blocks));
  LOG(INFO) << "[rename_ops_for_file] Added op#" << ops.size()
            << ": delete " << src_znode;

  ops.push_back(zk->build_delete_op(src_znode));
  return true;
}

bool ZkNnClient::rename_ops_for_dir(const std::string &src,
                  const std::string &dst,
                  std::vector<std::shared_ptr<ZooOp>> &ops) {
  // Create a znode for the dst dir
  int error_code = 0;
  auto data = std::vector<std::uint8_t>();
  std::string src_znode = ZookeeperFilePath(src);
  std::string dst_znode = ZookeeperFilePath(dst);

  // Get the payload from the old filesystem znode for the src
  zk->get(src_znode, data, error_code);
  if (error_code != ZOK) {
    LOG(ERROR) << "[rename_ops_for_dir] Failed to get data from '"
           << src_znode
           << "' when renaming.";
    return false;
  }

  // Create a new znode in the filesystem for the dst
  LOG(INFO) << "[rename_ops_for_dir] rename_ops_for_dir - Added op#"
        << ops.size()
        << ": create "
        << dst_znode;
  ops.push_back(zk->build_create_op(dst_znode, data));

  // Iterate through the items in this dir
  auto children = std::vector<std::string>();
  if (!zk->get_children(src_znode, children, error_code)) {
    LOG(ERROR) << "[rename_ops_for_dir] Failed to get children of znode '"
           << src_znode
           << "' when renaming.";
    return false;
  }

  auto nested_dirs = std::vector<std::string>();
  for (auto child : children) {
    std::string child_path = src + "/" + child;
    FileZNode znode_data;
    read_file_znode(znode_data, child_path);
    if (znode_data.file_type == FileType::Dir) {
      LOG(INFO) << "[rename_ops_for_dir] Child: " << child << " is DIR";
      // Keep track of any nested directories
      nested_dirs.push_back(child);
    } else if (znode_data.file_type == FileType::File) {
      // Generate ops for each file in the dir
      LOG(INFO) << "[rename_ops_for_dir] Child: " << child << " is FILE";
      if (!rename_ops_for_file(src + "/" + child, dst + "/" + child, ops)) {
        return false;
      }
    } else {
      LOG(ERROR) << "[rename_ops_for_dir] Requested rename source: "
                 << child_path
                 << " is not a file or dir";
      return false;
    }
  }

  // Iterate through the found nested directories and generate ops for them
  LOG(INFO) << "Found " << nested_dirs.size() << " nested dirs";
  for (auto dir : nested_dirs) {
    LOG(INFO) << "[rename_ops_for_dir] Call recursively on: "
              << src + "/" + dir;
    if (!rename_ops_for_dir(src + "/" + dir, dst + "/" + dir, ops)) {
      return false;
    }
  }
  // Delete the old dir
  LOG(INFO) << "[rename_ops_for_dir] Added op#" << ops.size()
            << ": delete " << src_znode;
  ops.push_back(zk->build_delete_op(src_znode));

  return true;
}

/**
 * Checks that each block UUID in the wait_for_acks dir:
 *	 1. has REPLICATION_FACTOR many children
 *	 2. if the block UUID was created more than ACK_TIMEOUT milliseconds ago
 *   TODO: Add to header file
 * @return
 */
bool ZkNnClient::check_acks() {
  int error_code = 0;

  // Get the current block UUIDs that are waiting to be fully replicated
  // TODO(2016): serialize block_uuids as u_int64_t rather than strings
  auto block_uuids = std::vector<std::string>();
  // TODO(2016): Change all path constants in zk_client_common to NOT end in /
  if (!zk->get_children(WORK_QUEUES + std::string(WAIT_FOR_ACK),
              block_uuids, error_code)) {
    LOG(ERROR) << "[check_acks] ERROR CODE: "
           << error_code
           << " occurred in check_acks when getting children for "
           << WORK_QUEUES + std::string(WAIT_FOR_ACK);
    return false;
  }
  LOG(INFO) << "[check_acks] Checking acks for: " << block_uuids.size()
            << " blocks";

  for (auto block_uuid : block_uuids) {
    uint64_t block_id;
    std::stringstream strm(block_uuid);
    strm >> block_id;
    bool ec = ZkClientCommon::is_ec_block(block_id);
    LOG(INFO) << "[check_acks] Considering block: " << block_uuid;
    std::string block_path = WORK_QUEUES + std::string(WAIT_FOR_ACK_BACKSLASH)
                 + block_uuid;

    auto data = std::vector<std::uint8_t>();
    if (!zk->get(block_path, data, error_code)) {
      LOG(ERROR) << "[check_acks] Error getting payload at: " << block_path;
      return false;
    }
    int replication_factor = unsigned(data[0]);
    LOG(INFO) << "[check_acks] Replication factor for "
              << block_uuid
              << " is "
              << replication_factor;

    // Get the datanodes with have replicated this block
    auto existing_dn_replicas = std::vector<std::string>();
    if (!zk->get_children(block_path, existing_dn_replicas, error_code)) {
      LOG(ERROR) << "[check_acks] ERROR CODE: "
                 << error_code
                 << " occurred in check_acks when getting children for "
                 << block_path;
      return false;
    }
    LOG(INFO) << "[check_acks] Found "
              << existing_dn_replicas.size()
              << " replicas of "
              << block_uuid;

    int elapsed_time = ms_since_creation(block_path);
    if (elapsed_time < 0) {
      LOG(ERROR) << "[check_acks] Failed to get elapsed time";
    }

    if (existing_dn_replicas.size() == 0 && elapsed_time > ACK_TIMEOUT && !ec) {
      // Block is not available on any DNs and cannot be replicated.
      // Emit error and remove this block from wait_for_acks
      LOG(ERROR) << block_path << "[check_acks]  has 0 replicas! "
              "Delete from wait_for_acks.";
      if (!zk->delete_node(block_path, error_code)) {
        LOG(ERROR) << "[check_acks] Failed to delete: " << block_path;
        return false;
      }
      return false;

    } else if (existing_dn_replicas.size() < replication_factor
           && elapsed_time > ACK_TIMEOUT) {
      LOG(INFO) << "[check_acks] Not yet enough replicas after time out for "
                << block_uuid;
      // Block hasn't been replicated enough, request remaining replicas
      int replicas_needed = replication_factor - existing_dn_replicas.size();
      LOG(INFO) << "[check_acks] " << replicas_needed << " replicas are needed";

      std::vector<std::string> to_replicate;
      for (int i = 0; i < replicas_needed; i++) {
        to_replicate.push_back(block_uuid);
      }
      if (ec) {
        if (!recover_ec_blocks(to_replicate, error_code)) {
          LOG(ERROR) << "[check_acks] Failed to add necessary "
                  "items to ec_recover queue.";
          return false;
        }
      } else {
        if (!replicate_blocks(to_replicate, error_code)) {
          LOG(ERROR) << "Failed to add necessary items to replication queue.";
          return false;
        }
      }

    } else if (existing_dn_replicas.size() == replication_factor) {
      LOG(INFO) << "[check_acks] Enough replicas have been made, "
              "no longer need to wait on "
                << block_path;
      if (!zk->recursive_delete(block_path, error_code)) {
        LOG(ERROR) << "[check_acks] Failed to delete: " << block_path;
        return false;
      }
    } else {
      LOG(INFO) << "[check_acks] Not enough replicas, but still time left"
                << block_path;
    }
  }
  return true;
}

bool ZkNnClient::recover_ec_blocks(
                      const std::vector<std::string> &to_ec_recover, int err) {
  std::vector<std::shared_ptr<ZooOp>> ops;
  std::vector<zoo_op_result> results;

  for (auto rec : to_ec_recover) {
    std::string read_from;
    std::vector<std::string> target_dn;
    std::uint64_t block_id;
    std::stringstream strm(rec);
    strm >> block_id;
    uint64_t blocksize;
    if (!get_block_size(block_id, blocksize)) {
      LOG(ERROR) << "[recover_ec_blocks] Replicate could not read "
              "the block size for block: "
                 << block_id;
      return false;
    }
    auto excluded = std::vector<std::string>();
    if (!find_datanode_for_block(target_dn, excluded, block_id, 1, blocksize)
        || target_dn.size() == 0) {
      LOG(ERROR) << "[recover_ec_blocks] Failed to find datanode "
              "for this block! " << rec;
      return false;
    }
    auto queue = EC_RECOVER_QUEUES + target_dn[0];
    auto rec_item = util::concat_path(queue, rec);
    ops.push_back(zk->build_create_op(rec_item, ZKWrapper::EMPTY_VECTOR));
  }

  // We do not need to sync this multi-op immediately
  if (!zk->execute_multi(ops, results, err, false)) {
    LOG(ERROR) << "[recover_ec_blocks] Failed to execute multiop "
            "for recover_ec_blocks";
    return false;
  }
}


bool ZkNnClient::replicate_blocks(const std::vector<std::string> &to_replicate,
                  int err) {
  std::vector<std::shared_ptr<ZooOp>> ops;
  std::vector<zoo_op_result> results;

  for (auto repl : to_replicate) {
    std::string read_from;
    std::vector<std::string> target_dn;
    std::uint64_t block_id;
    std::stringstream strm(repl);
    strm >> block_id;
    uint64_t blocksize;
    if (!get_block_size(block_id, blocksize)) {
      LOG(ERROR) << "[replicate_blocks] Replicate could not read "
              "the block size for block: "
             << block_id;
      return false;
    }
    int err;
    auto existing_dn_replicas = std::vector<std::string>();
    find_all_datanodes_with_block(block_id, existing_dn_replicas, err);
    if (!find_datanode_for_block(target_dn, existing_dn_replicas, block_id,
                                 1, blocksize) || target_dn.size() == 0) {
      LOG(ERROR) << "[replicate_blocks] Failed to find datanode "
              "for this block! " << repl;
      return false;
    }
    auto queue = REPLICATE_QUEUES + target_dn[0];
    auto repl_item = util::concat_path(queue, repl);
    ops.push_back(zk->build_create_op(repl_item, ZKWrapper::EMPTY_VECTOR));
  }

  // We do not need to sync this multi-op immediately
  if (!zk->execute_multi(ops, results, err, false)) {
    LOG(ERROR) << "[replicate_blocks] Failed to execute multiop "
            "for replicate_blocks";
    return false;
  }

  return true;
}

bool ZkNnClient::find_all_datanodes_with_block(
    const uint64_t &block_uuid,
    std::vector<std::string> &rdatanodes, int &error_code) {
  std::string block_loc_path = get_block_metadata_path(block_uuid);

  if (!zk->get_children(block_loc_path, rdatanodes, error_code)) {
    LOG(ERROR) << "[find_all_datanodes_with_block] Failed to get "
            "children of: " << block_loc_path;
    return false;
  }
  if (rdatanodes.size() < 1) {
    LOG(ERROR) << "[find_all_datanodes_with_block] There are no "
            "datanodes with a replica of block "
               << block_uuid;
    return false;
  }
  return true;
}

int ZkNnClient::ms_since_creation(std::string &path) {
  int error;
  struct Stat stat;
  if (!zk->get_info(path, stat, error)) {
    LOG(ERROR) << "[ms_since_creation] Failed to get info for: " << path;
    return -1;
  }
  LOG(INFO) << "[ms_since_creation] Creation time of " << path << " was: "
            << stat.ctime << " ms ago";
  uint64_t current_time = current_time_ms();
  LOG(INFO) << "[ms_since_creation] Current time is: " << current_time << "ms";
  int elapsed = current_time - stat.ctime;
  LOG(INFO) << "[ms_since_creation] Elapsed ms: " << elapsed;
  return elapsed;
}

/**
* Returns the current timestamp in milliseconds
*/
uint64_t ZkNnClient::current_time_ms() {
  // http://stackoverflow.com/questions/19555121/how-to-get-current-time
  // stamp-in-milliseconds-since-1970-just-the-way-java-gets
  struct timeval tp;
  // Get current timestamp
  gettimeofday(&tp, NULL);
  // Convert to milliseconds
  return (uint64_t) tp.tv_sec * 1000L + tp.tv_usec / 1000;
}

bool ZkNnClient::buildDatanodeInfoProto(DatanodeInfoProto *dn_info,
                    const std::string &data_node) {
  int error_code;

  std::vector<std::string> split_address;
  boost::split(split_address, data_node, boost::is_any_of(":"));
  assert(split_address.size() == 2);

  auto data = std::vector<std::uint8_t>();
  if (zk->get(HEALTH_BACKSLASH + data_node + STATS, data,
        error_code, sizeof(zkclient::DataNodePayload))) {
    LOG(ERROR) << "[buildDatanodeInfoProto] Getting data node stats "
            "failed with " << error_code;
  }

  zkclient::DataNodePayload *payload = (zkclient::DataNodePayload *) (&data[0]);

  DatanodeIDProto *id = dn_info->mutable_id();
  dn_info->set_location("/fixlocation");
  id->set_ipaddr(split_address[0]);
  // TODO(2016): Fill out with the proper value
  id->set_hostname("localhost");
  id->set_datanodeuuid("1234");
  id->set_xferport(payload->xferPort);
  id->set_infoport(50020);
  id->set_ipcport(payload->ipcPort);

  return true;
}

bool ZkNnClient::buildTokenProto(hadoop::common::TokenProto *token) {
  token->set_identifier("open");
  token->set_password("sesame");
  token->set_kind("foo");
  token->set_service("bar");
  return true;
}

bool ZkNnClient::buildExtendedBlockProto(ExtendedBlockProto *eb,
                     const std::uint64_t &block_id,
                     const uint64_t &block_size) {
  eb->set_poolid("0");
  eb->set_blockid(block_id);
  eb->set_generationstamp(1);
  eb->set_numbytes(block_size);
  return true;
}

bool ZkNnClient::lease_expired(std::string lease_holder_client) {
  bool exists;
  int error_code;
  if (!zk->exists(ClientZookeeperPath(lease_holder_client), exists,
                  error_code)) {
    LOG(ERROR) << "[lease_expired] Failed to check whether the client " +
                    ClientZookeeperPath(lease_holder_client) << " exist.";
    return false;
  }
  if (!exists) {
    // Thanks god it is for sure dead.
    return true;
  }
  uint64_t timestamp = get_client_lease_timestamp(lease_holder_client);
  if (timestamp - current_time_ms() < EXPIRATION_TIME) {
    return false;
  } else {
    if (!zk->delete_node(ClientZookeeperPath(lease_holder_client),
                         error_code)) {
      LOG(ERROR) << "[lease_expired] Failed to delete client " +
                      ClientZookeeperPath(lease_holder_client) << ".";
      return false;
    }
  }
}

uint64_t ZkNnClient::get_client_lease_timestamp(std::string client_name) {
  ClientInfo clientInfo;
  read_znode_data(clientInfo, ClientZookeeperPath(client_name));
  return clientInfo.timestamp;
}
}  // namespace zkclient

#endif
