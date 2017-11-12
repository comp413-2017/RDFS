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
#include <zk_nn_client.h>
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
using hadoop::hdfs::ErasureCodingPolicyProto;
using hadoop::hdfs::ECSchemaProto;
using hadoop::hdfs::GetErasureCodingPoliciesRequestProto;
using hadoop::hdfs::GetErasureCodingPoliciesResponseProto;

namespace zkclient {

ZkNnClient::ZkNnClient(std::string zkIpAndAddress) :
    ZkClientCommon(zkIpAndAddress) {
  mkdir_helper("/", false);
  populateDefaultECProto();
}

ZkNnClient::ZkNnClient(std::shared_ptr<ZKWrapper> zk_in) :
    ZkClientCommon(zk_in) {
  mkdir_helper("/", false);
  populateDefaultECProto();
}

void ZkNnClient::populateDefaultECProto() {
  DEFAULT_EC_SCHEMA.set_parityunits(DEFAULT_PARITY_UNITS);
  DEFAULT_EC_SCHEMA.set_dataunits(DEFAULT_DATA_UNITS);
  DEFAULT_EC_SCHEMA.set_codecname(DEFAULT_EC_CODEC_NAME);
  RS_SOLOMON_PROTO.set_name(DEFAULT_EC_POLICY);
  RS_SOLOMON_PROTO.set_allocated_schema(&DEFAULT_EC_SCHEMA);
  RS_SOLOMON_PROTO.set_cellsize(DEFAULT_EC_CELLCIZE);
  RS_SOLOMON_PROTO.set_id(DEFAULT_EC_ID);
}

/*
 * A simple print function that will be triggered when
 * namenode loses a heartbeat
 */
void notify_delete() {
  printf("No heartbeat, no childs to retrieve\n");
}

void ZkNnClient::register_watches() {
  int err;
  std::vector<std::string> children = std::vector<std::string>();
  if (!(zk->wget_children(HEALTH, children, watcher_health, this, err))) {
    // TODO(2016): Handle error
    LOG(ERROR) << "[In register_watchers], wget failed " << err;
  }

  for (int i = 0; i < children.size(); i++) {
    LOG(INFO) << "[In register_watches] Attaching child to "
              << children[i]
              << ", ";
    std::vector<std::string> ephem = std::vector<std::string>();
    if (!(zk->wget_children(HEALTH_BACKSLASH + children[i], ephem,
                            watcher_health_child, this, err))) {
      // TODO(2016): Handle error
      LOG(ERROR) << "[In register_watchers], wget failed " << err;
    }
  }
}

/**
 * Static
 */
void ZkNnClient::watcher_health(zhandle_t *zzh, int type, int state,
                                const char *path, void *watcherCtx) {
  LOG(INFO) << "Health watcher triggered on " << path;

  ZkNnClient *cli = reinterpret_cast<ZkNnClient *>(watcherCtx);
  auto zk = cli->zk;

  int err;
  std::vector<std::string> children = std::vector<std::string>();
  if (!(zk->wget_children(HEALTH, children, ZkNnClient::watcher_health,
                          watcherCtx, err))) {
    // TODO(2016): Handle error
    LOG(ERROR) << "[In register_watchers], wget failed " << err;
  }

  for (int i = 0; i < children.size(); i++) {
    LOG(INFO) << "[In register_watches] Attaching child to "
              << children[i]
              << ", ";
    std::vector<std::string> ephem = std::vector<std::string>();
    if (!(zk->wget_children(HEALTH_BACKSLASH + children[i],
                            ephem,
                            ZkNnClient::watcher_health_child,
                            watcherCtx,
                            err))) {
      // TODO(2016): Handle error
      LOG(ERROR) << "[In register_watchers], wget failed " << err;
    }
  }
}

/**
 * Static
 */
void ZkNnClient::watcher_health_child(zhandle_t *zzh, int type, int state,
                                      const char *raw_path, void *watcherCtx) {
  LOG(INFO) << "Health child watcher triggered on " << raw_path;

  ZkNnClient *cli = reinterpret_cast<ZkNnClient *>(watcherCtx);
  auto zk = cli->zk;
  auto path = zk->removeZKRoot(std::string(raw_path));

  std::vector<std::string> children;

  LOG(INFO) << "[health child] Watcher triggered on path '" << path;
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
    LOG(ERROR) << " An error occurred while trying to lock " << path;
    return;
  }

  ret = zk->get_children(path, children, err);

  if (!ret) {
    LOG(ERROR) << " Failed to get children";
    goto unlock;
  }

  /* Check for heartbeat */
  for (int i = 0; i < children.size(); i++) {
    if (children[i] == "heartbeat") {
      /* Heartbeat exists! Don't do anything */
      LOG(INFO) << " Heartbeat found while deleting";
      goto unlock;
    }
  }
  children.clear();

  /* Get all replication queue items for this datanode, save it */
  if (!zk->get_children(repl_q_path.c_str(), children, err)) {
    LOG(ERROR) << " Failed to get dead datanode's replication queue";
    goto unlock;
  }
  for (auto child : children) {
    to_replicate.push_back(child);
  }
  children.clear();

  /* Get all ec_recover queue items for this datanode, save it */
  if (!zk->get_children(ec_rec_q_path.c_str(), children, err)) {
    LOG(ERROR) << " Failed to get dead datanode's ec_recover queue";
    goto unlock;
  }
  for (auto child : children)
    to_ec_recover.push_back(child);
  children.clear();

  /* Delete all work queues for this datanode */
  if (!zk->recursive_delete(repl_q_path, err)) {
    LOG(ERROR) << " Failed to delete dead datanode's replication queue";
    goto unlock;
  }
  if (!zk->recursive_delete(delete_q_path, err)) {
    LOG(ERROR) << " Failed to delete dead datanode's delete queue";
    goto unlock;
  }
  if (!zk->recursive_delete(ec_rec_q_path, err)) {
    LOG(ERROR) << " Failed to delete dead datanode's ec_recover queue";
    goto unlock;
  }

  /* Put every block from /blocks on replication queue as well as any saved
   * items from replication queue
   */

  if (!zk->get_children(block_path.c_str(), children, err)) {
    LOG(ERROR) << " Failed to get children for blocks";
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
    LOG(ERROR) << "Failed to clean up dead datanode.";
  }

  /* Push all blocks needing to be replicated onto the queue */
  if (!cli->replicate_blocks(to_replicate, err)) {
    LOG(ERROR) << "Failed to push all items on to replication queues!";
    goto unlock;
  }
  /* Push all ec blocks needing to be recovered onto the queue */
  if (!cli->recover_ec_blocks(to_ec_recover, err)) {
    LOG(ERROR) << "Failed to push all items on to ec_recover queues!";
    goto unlock;
  }
  unlock:
  /* Unlock the znode */
  if (race_lock.unlock()) {
    LOG(ERROR) << " An error occurred while trying to unlock " << path;
  }
}

bool ZkNnClient::file_exists(const std::string &path) {
  bool exists;
  int error_code;
  if (zk->exists(ZookeeperFilePath(path), exists, error_code)) {
    return exists;
  } else {
//    return false;
  }
}

bool ZkNnClient::get_block_size(const u_int64_t &block_id,
                                uint64_t &blocksize) {
  int error_code;
  std::string block_path = get_block_metadata_path(block_id);

  BlockZNode block_data;
  std::vector<std::uint8_t> data(sizeof(block_data));
  if (!zk->get(block_path, data, error_code)) {
    LOG(ERROR) << "We could not read the block at " << block_path;
    return false;
  }
  std::uint8_t *buffer = &data[0];
  memcpy(&block_data, &data[0], sizeof(block_data));

  blocksize = block_data.block_size;
  LOG(INFO) << "Block size of: " << block_path << " is " << blocksize;
  return true;
}

void ZkNnClient::set_node_policy(char policy) {
  ZkNnClient::policy = policy;
}

char ZkNnClient::get_node_policy() {
  return ZkNnClient::policy;
}


// --------------------------- PROTOCOL CALLS -------------------------------

void ZkNnClient::read_file_znode(FileZNode &znode_data,
                                 const std::string &path) {
  int error_code;
  std::vector<std::uint8_t> data(sizeof(znode_data));
  if (!zk->get(ZookeeperFilePath(path), data, error_code)) {
    LOG(ERROR) << "We could not read the file znode at " << path;
    return;  // don't bother reading the data
  }
  std::uint8_t *buffer = &data[0];
  memcpy(&znode_data, buffer, sizeof(znode_data));
}

void ZkNnClient::file_znode_struct_to_vec(FileZNode *znode_data,
                                          std::vector<std::uint8_t> &data) {
  memcpy(&data[0], znode_data, sizeof(*znode_data));
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
      LOG(INFO) << "Had to sync: previous block failed";
      // If we failed initially attempt to sync the changes, then check again
      zk->flush(zk->prepend_zk_root(block_metadata_path), true);
      if (zk->get_children(block_metadata_path,
                           children, error_code)) {
        return children.size() >= MIN_REPLICATION;
      }
    }
  }
  return false;
}


bool ZkNnClient::add_block(AddBlockRequestProto &req,
                           AddBlockResponseProto &res) {
  // make sure previous addBlock operation has completed
  auto prev_id = req.previous().blockid();
  if (!previousBlockComplete(prev_id)) {
    LOG(ERROR) << "Previous Add Block Operation has not finished";
    return false;
  }

  // Build a new block for the response
  auto block = res.mutable_block();

  // TODO(2016): Make sure we are appending / not appending ZKPath at every step
  const std::string file_path = req.src();

  LOG(INFO) << "Attempting to add block to existing file " << file_path;

  FileZNode znode_data;
  if (!file_exists(file_path)) {
    LOG(ERROR) << "Requested file " << file_path << " does not exist";
    return false;
  }
  read_file_znode(znode_data, file_path);
  // Assert that the znode we want to modify is a file
  if (znode_data.filetype != IS_FILE) {
    LOG(ERROR) << "Requested file " << file_path << " is not a file";
    return false;
  }

  uint32_t replication_factor = znode_data.replication;
  uint64_t block_size = znode_data.blocksize;
  assert(block_size > 0);

  std::uint64_t block_id;
  auto data_nodes = std::vector<std::string>();

  if (!znode_data.isEC) {
      add_block(file_path, block_id, data_nodes, replication_factor);
  } else {  // case when some EC policy is used.
      // TODO(Nate): generate a block group and each part of a block group.
      // TODO(Nate): use the hierarchical naming scheme.
  }

  block->set_offset(0);  // TODO(2016): Set this
  block->set_corrupt(false);

  buildExtendedBlockProto(block->mutable_b(), block_id, block_size);

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
                               AbandonBlockResponseProto &res) {
  const std::string &file_path = req.src();
  // I believe this is the lease holder?
  const std::string &holder = req.holder();
  LOG(INFO) << "Attempting to abandon block";

  // uint64 generationStamp (as well as the optional uint64 numBytes)
  const ExtendedBlockProto &block = req.b();
  const std::string poolId = block.poolid();
  const uint64_t blockId = block.blockid();
  const uint64_t generation_stamp = block.generationstamp();
  LOG(INFO) << "Requested to abandon block: " << blockId;
  LOG(INFO) << "This block is in pool " << poolId;
  LOG(INFO) << "Its generation stamp is " << generation_stamp;

  const std::string block_id_str = std::to_string(blockId);
  LOG(INFO) << "...Also, converted blockid to a string: " << block_id_str;

  LOG(INFO) << "Checking file exists: " << file_path;

  // Double check the file exists first
  FileZNode znode_data;
  if (!file_exists(file_path)) {
    LOG(ERROR) << "Requested file " << file_path << " does not exist";
    return false;
  }
  read_file_znode(znode_data, file_path);
  // Assert that the znode we want to modify is a file
  if (znode_data.filetype != IS_FILE) {
    LOG(ERROR) << "Requested file " << file_path << " is not a file";
    return false;
  }
  LOG(INFO) << "File exists. Building multi op to abandon block";

  // Find the last block
  int error_code;
  std::vector<std::string> sorted_fs_znodes;
  if (!zk->get_children(ZookeeperBlocksPath(file_path),
                        sorted_fs_znodes, error_code)) {
    LOG(ERROR) << "Failed getting children of "
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
    LOG(ERROR) << "Could not find datandoes with the block";
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

  LOG(INFO) << "Built multi op. Executing multi op to abandon block... "
            << file_path;
  if (!zk->execute_multi(ops, results, err)) {
    LOG(ERROR) << "Failed to write the abandon_block multiop, "
        "ZK state was not changed";
    ZKWrapper::print_error(err);
    for (int i = 0; i < results.size(); i++) {
      LOG(ERROR) << "\t MULTIOP #" << i << " ERROR CODE: " << results[i].err;
    }
    return false;
  }
  return true;
}

ZkNnClient::GetFileInfoResponse ZkNnClient::get_info(
    GetFileInfoRequestProto &req, GetFileInfoResponseProto &res) {
  const std::string &path = req.src();

  if (file_exists(path)) {
    LOG(INFO) << "File exists";
    // read the node into the file node struct
    FileZNode znode_data;
    read_file_znode(znode_data, path);

    // set the file status in the get file info response res
    HdfsFileStatusProto *status = res.mutable_fs();

    set_file_info(status, path, znode_data);
    LOG(INFO) << "Got info for file ";
    return GetFileInfoResponse::Ok;
  } else {
    LOG(INFO) << "No file to get info for";
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
    LOG(ERROR) << "Creating file znode at " << path;
    {
      LOG(INFO) << znode_data->replication << "\n";
      LOG(INFO) << znode_data->owner << "\n";
      LOG(INFO) << "is this file ec? " << znode_data->isEC << "\n";
      LOG(INFO) << "size of znode is " << sizeof(*znode_data) << "\n";
    }
    // serialize struct to byte vector
    std::vector<std::uint8_t> data(sizeof(*znode_data));
    file_znode_struct_to_vec(znode_data, data);
    // crate the node in zookeeper
    if (!zk->create(ZookeeperFilePath(path), data, error_code, false)) {
      LOG(ERROR) << "Create failed with error code " << error_code;
      return false;
      // TODO(2016): handle error
    }
    if (znode_data->filetype == 2) {
        std::vector<std::uint8_t> data2(sizeof(*znode_data));
        if (!zk->create(ZookeeperBlocksPath(path), data2, error_code, false)) {
            LOG(ERROR) << "Create failed with error code " << error_code;
            return false;
            // TODO(2016): handle error
        }
    }
    return true;
  }
  return false;
}

bool ZkNnClient::blockDeleted(uint64_t uuid, std::string id) {
  int error_code;
  bool exists;

  LOG(INFO) << "DataNode deleted a block with UUID " << std::to_string(uuid);

  auto ops = std::vector<std::shared_ptr<ZooOp>>();
  std::string block_metadata_path = get_block_metadata_path(uuid);
  // Delete block locations
  if (zk->exists(block_metadata_path +
      "/" + id, exists, error_code)) {
    if (exists) {
      ops.push_back(zk->build_delete_op(block_metadata_path +
          "/" + id));
      std::vector<std::string> children = std::vector<std::string>();
      if (!zk->get_children(block_metadata_path,
                            children, error_code)) {
        LOG(ERROR) << "getting children failed";
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
    LOG(ERROR) << "Failed multiop when deleting block" << std::to_string(uuid);
    for (int i = 0; i < results.size(); i++) {
      LOG(ERROR) << "\t MULTIOP #" << i << " ERROR CODE: " << results[i].err;
    }
    return false;
  }
  return true;
}


ZkNnClient::DeleteResponse ZkNnClient::destroy_helper(const std::string &path,
                                std::vector<std::shared_ptr<ZooOp>> &ops) {
  LOG(INFO) << "Destroying " << path;

  if (!file_exists(path)) {
    LOG(ERROR) << path << " does not exist";
    return DeleteResponse::FileDoesNotExist;
  }

  int error_code;
  FileZNode znode_data;
  read_file_znode(znode_data, path);
  std::vector<std::string> children;
  LOG(INFO) << "read file znode successful";
  if (znode_data.filetype == IS_DIR) {
    LOG(ERROR) << "deleting file";
    if (!zk->get_children(ZookeeperFilePath(path), children, error_code)) {
          LOG(FATAL) << "Failed to get children for " << path;
          return DeleteResponse::FailedChildRetrieval;
    }
    for (auto& child : children) {
      auto child_path = util::concat_path(path, child);
      auto status = destroy_helper(child_path, ops);
      if (status != DeleteResponse::Ok) {
        return status;
      }
    }
  } else if (znode_data.filetype == IS_FILE) {
    LOG(ERROR) << "deleting file";
    if (!zk->get_children(ZookeeperBlocksPath(path), children, error_code)) {
      LOG(FATAL) << "Failed to get children for " << path;
      return DeleteResponse::FailedChildRetrieval;
    }
    if (znode_data.under_construction == FileStatus::UnderConstruction) {
      LOG(ERROR) << path << " is under construction, so it cannot be deleted.";
      return DeleteResponse::FileUnderConstruction;
    }
    for (auto &child : children) {
      auto child_path = util::concat_path(ZookeeperBlocksPath(path), child);
      LOG(INFO) << "  path: " <<
                              ZookeeperBlocksPath(path) << " child: " << child;
      child_path = child_path;
      LOG(ERROR) << " child path: " << child_path;
      ops.push_back(zk->build_delete_op(child_path));
      std::vector<std::uint8_t> block_vec;
      std::uint64_t block;
      if (!zk->get(child_path, block_vec, error_code, sizeof(block))) {
        return DeleteResponse::FailedBlockRetrieval;
      }
      block = *reinterpret_cast<std::uint64_t *>(block_vec.data());
      std::vector<std::string> datanodes;

      if (!zk->get_children(get_block_metadata_path(block),
                            datanodes, error_code)) {
        LOG(ERROR) << "Failed getting datanode locations for block: "
                   << block
                   << " with error: "
                   << error_code;
        return DeleteResponse::FailedDataNodeRetrieval;
      }
      // push delete commands onto ops
      for (auto &dn : datanodes) {
        auto delete_queue = util::concat_path(DELETE_QUEUES, dn);
        auto delete_item = util::concat_path(delete_queue, "block-");
        ops.push_back(zk->build_create_op(delete_item, block_vec,
                                          ZOO_SEQUENCE));
        blockDeleted(block, dn);
      }
    }
    ops.push_back(zk->build_delete_op(ZookeeperBlocksPath(path)));
  }
  ops.push_back(zk->build_delete_op(ZookeeperFilePath(path)));
  return DeleteResponse::Ok;
}

void ZkNnClient::complete(CompleteRequestProto& req,
                          CompleteResponseProto& res) {
  // TODO(nate):  A call to complete() will not return true until
  // all the file's blocks have been replicated the minimum number of times.

  int error_code;
  // change the under construction bit
  const std::string& src = req.src();
  FileZNode znode_data;
  read_file_znode(znode_data, src);
  znode_data.under_construction = FileStatus::FileComplete;
  // set the file length
  uint64_t file_length = 0;
  auto file_blocks = std::vector<std::string>();
  if (!zk->get_children(ZookeeperBlocksPath(src), file_blocks, error_code)) {
    LOG(ERROR) << "Failed getting children of "
               << ZookeeperBlocksPath(src)
               << " with error: "
               << error_code;
    res.set_result(false);
    return;
  }
  if (file_blocks.size() == 0) {
    LOG(ERROR) << "No blocks found for file " << ZookeeperFilePath(src);
    res.set_result(true);
  }
  // TODO(2016): This loop could be two multi-ops instead
  for (auto file_block : file_blocks) {
    auto data = std::vector<std::uint8_t>();
    if (!zk->get(ZookeeperBlocksPath(src) + "/" + file_block, data,
                 error_code, sizeof(uint64_t))) {
      LOG(ERROR) << "Failed to get "
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
      LOG(ERROR) << "Failed to get "
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
          << " complete could not change the construction bit and file length";
      res.set_result(false);
      return;
  }
  res.set_result(true);
}

/**
 * Go down directories recursively. If a child is a file, then put its deletion on a queue.
 * Files delete themselves, but directories are deleted by their parent (so root can't be deleted)
 */
ZkNnClient::DeleteResponse ZkNnClient::destroy(DeleteRequestProto &request,
                         DeleteResponseProto &response) {
  int error_code;
  const std::string &path = request.src();
  bool recursive = request.recursive();
  response.set_result(true);

  if (!file_exists(path)) {
    LOG(ERROR) << "Cannot delete "
               << path
               << " because it doesn't exist.";
    response.set_result(false);
    return DeleteResponse::FileDoesNotExist;
  }

  FileZNode znode_data;
  read_file_znode(znode_data, path);

  if (znode_data.filetype == IS_FILE
      && znode_data.under_construction == FileStatus::UnderConstruction) {
    LOG(ERROR) << "Cannot delete "
               << path
               << " because it is under construction.";
    response.set_result(false);
    return DeleteResponse::FileUnderConstruction;
  }

  if (znode_data.filetype == IS_DIR && !recursive) {
    LOG(ERROR) << "Cannot delete "
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

  LOG(INFO) << "Deleting multiop has "
          << ops.size()
          << " operations. Executing...";
  std::vector<zoo_op_result> results;
  if (!zk->execute_multi(ops, results, error_code)) {
    LOG(ERROR) <<
               "Failed to execute multi op to delete " <<
                path << " Co: " << error_code;
    response.set_result(false);

    for (int i = 0; i < results.size(); i++) {
      LOG(ERROR) << "\t MULTIOP #" << i << " ERROR CODE: " << results[i].err;
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
  LOG(INFO) << "Trying to create file " << path;
  const std::string &owner = request.clientname();
  bool create_parent = request.createparent();
  std::uint64_t blocksize = request.blocksize();
  std::uint32_t replication = request.replication();
  std::uint32_t createflag = request.createflag();
  const std::string &inputECPolicyName = request.ecpolicyname();

  if (file_exists(path)) {
    // TODO(2016) solve this issue of overwriting files
    LOG(ERROR) << "File already exists";
    return CreateResponse::FileAlreadyExists;
  }

  // If we need to create directories, do so
  if (create_parent) {
    std::string directory_paths = "";
    std::vector<std::string> split_path;
    boost::split(split_path, path, boost::is_any_of("/"));
    LOG(INFO) << split_path.size();
    for (int i = 1; i < split_path.size() - 1; i++) {
      directory_paths += ("/" + split_path[i]);

      // try and make all the parents
      if (mkdir_helper(directory_paths, true) !=
          ZkNnClient::MkdirResponse::Ok)
        return CreateResponse::FailedMkdir;
    }
  }

  // Now create the actual file which will hold blocks
  FileZNode znode_data;
  znode_data.length = 0;
  znode_data.under_construction = FileStatus::UnderConstruction;
  uint64_t mslong = current_time_ms();
  znode_data.access_time = mslong;
  znode_data.modification_time = mslong;
  snprintf(znode_data.owner, strlen(znode_data.owner), owner.c_str());
  snprintf(znode_data.group, strlen(znode_data.group), owner.c_str());
  znode_data.replication = replication;
  znode_data.blocksize = blocksize;
  znode_data.filetype = IS_FILE;
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
                                              RenameResponseProto& res) {
  std::string file_path = req.src();

  FileZNode znode_data;
  read_file_znode(znode_data, file_path);
  if (!file_exists(file_path)) {
    LOG(ERROR) << "Requested rename source: " << file_path << " does not exist";
    res.set_result(false);
      return RenameResponse::FileDoesNotExist;
  }

  auto ops = std::vector<std::shared_ptr<ZooOp>>();
  if (znode_data.filetype == IS_DIR) {
    if (!rename_ops_for_dir(req.src(), req.dst(), ops)) {
      LOG(ERROR) << "Failed to generate rename operatons for: " << file_path;
      res.set_result(false);
        return RenameResponse::RenameOpsFailed;
    }

  } else if (znode_data.filetype == IS_FILE) {
    if (!rename_ops_for_file(req.src(), req.dst(), ops)) {
      LOG(ERROR) << "Failed to generate rename operatons for: " << file_path;
      res.set_result(false);
        return RenameResponse::RenameOpsFailed;
    }

  } else {
    LOG(ERROR) << "Requested rename source: "
               << file_path
               << " is not a file or dir";
    res.set_result(false);
      return RenameResponse::InvalidType;
  }

  LOG(INFO) << "Renameing multiop has "
            << ops.size()
            << " operations. Executing...";

  int error_code;
  std::vector<zoo_op_result> results;
  if (!zk->execute_multi(ops, results, error_code)) {
    LOG(ERROR) << "Failed multiop when renaming: '"
               << req.src()
               << "' to '"
               << req.dst()
               << "'";
    for (int i = 0; i < results.size(); i++) {
      LOG(ERROR) << "\t MULTIOP #" << i << " ERROR CODE: " << results[i].err;
    }
    res.set_result(false);
  } else {
    LOG(INFO) << "Successfully exec'd multiop to rename "
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
  znode_data->access_time = mslong;  // TODO(2016) what are these
  znode_data->modification_time = mslong;
  znode_data->blocksize = 0;
  znode_data->replication = 0;
  znode_data->filetype = IS_DIR;
  znode_data->isEC = false;
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
  LOG(ERROR) << "mkdir_helper called with input " << path;
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
      LOG(INFO) << "[in mkdir_helper] " << p_path;
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
      } else {
        LOG(INFO) << "mkdir_helper is trying to create";
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

ZkNnClient::ListingResponse ZkNnClient::get_listing(GetListingRequestProto &req,
                                    GetListingResponseProto &res) {
  int error_code;

  const std::string &src = req.src();
    const int start_after = req.startafter();
    const bool need_location = req.needlocation();

  DirectoryListingProto *listing = res.mutable_dirlist();

  // if src is a file then just return that file with remaining = 0
  // otherwise return first 1000 files in src dir starting at start_after
  // and set remaining to the number left after that first 1000
  // TODO(2016) handle lengths of more than 1000 files
  if (file_exists(src)) {
    FileZNode znode_data;
    read_file_znode(znode_data, src);
    if (znode_data.filetype == IS_FILE) {
      HdfsFileStatusProto *status = listing->add_partiallisting();
      set_file_info(status, src, znode_data);
      if (need_location) {
          LocatedBlocksProto *blocks = status->mutable_locations();
          get_block_locations(src, 0, znode_data.length, blocks);
      }
    } else {
      std::vector<std::string> children;
      if (!zk->get_children(ZookeeperFilePath(src), children, error_code)) {
        LOG(FATAL) << "Failed to get children for " << ZookeeperFilePath(src);
        return ListingResponse::FailedChildRetrieval;
      } else {
        for (auto &child : children) {
          auto child_path = util::concat_path(src, child);
          FileZNode child_data;
          read_file_znode(child_data, child_path);
          HdfsFileStatusProto *status = listing->add_partiallisting();
          set_file_info(status, child_path, child_data);
          // set up the value for LocatedBlocksProto
          // LocatedBlocksProto *blocklocation = status->set_location();
          // GetBlockLocationsRequestProto location_req;
          if (need_location) {
              LocatedBlocksProto *blocks = status->mutable_locations();
              get_block_locations(child_path, 0, child_data.length, blocks);
          }
        }
      }
    }
    listing->set_remainingentries(0);
  } else {
    LOG(ERROR) << "File does not exist with name " << src;
    return ListingResponse::FileDoesNotExist;
  }
  return ListingResponse::Ok;
}

void ZkNnClient::get_block_locations(GetBlockLocationsRequestProto &req,
                                     GetBlockLocationsResponseProto &res) {
  const std::string &src = req.src();
  google::protobuf::uint64 offset = req.offset();
  google::protobuf::uint64 length = req.length();
  LocatedBlocksProto *blocks = res.mutable_locations();
  get_block_locations(src, offset, length, blocks);
}

void ZkNnClient::get_block_locations(const std::string &src,
                                     google::protobuf::uint64 offset,
                                     google::protobuf::uint64 length,
                                     LocatedBlocksProto *blocks) {
  int error_code;
  const std::string zk_path = ZookeeperBlocksPath(src);

  FileZNode znode_data;
  read_file_znode(znode_data, src);

  blocks->set_underconstruction(false);
  blocks->set_islastblockcomplete(true);
  blocks->set_filelength(znode_data.length);

  uint64_t block_size = znode_data.blocksize;

  LOG(INFO) << "Block size of " << zk_path << " is " << block_size;

  auto sorted_blocks = std::vector<std::string>();

  // TODO(2016): Make more efficient
  if (!zk->get_children(zk_path, sorted_blocks, error_code)) {
    LOG(ERROR) << "Failed getting children of "
               << zk_path
               << " with error: "
               << error_code;
  }

  std::sort(sorted_blocks.begin(), sorted_blocks.end());

  uint64_t size = 0;
  for (auto sorted_block : sorted_blocks) {
    LOG(INFO) << "Considering block " << sorted_block;
    if (size > offset + length) {
      // at this point the start of the block is at a higher
      // offset than the segment we want
      LOG(INFO) << "Breaking at block " << sorted_block;
      break;
    }
    if (size + block_size >= offset) {
      auto data = std::vector<uint8_t>();
      if (!zk->get(zk_path + "/" + sorted_block, data,
                   error_code, sizeof(uint64_t))) {
        LOG(ERROR) << "Failed to get "
                   << zk_path << "/"
                   << sorted_block
                   << " info: "
                   << error_code;
        return;  // TODO(2016): Signal error
      }
      uint64_t block_id = *reinterpret_cast<uint64_t *>(&data[0]);
      LOG(INFO) << "Found block " << block_id << " for " << zk_path;

      // TODO(2016): This block of code should be moved to a function,
      // repeated with add_block
      LocatedBlockProto *located_block = blocks->add_blocks();
      located_block->set_corrupt(0);
      // TODO(2016): This offset may be incorrect
      located_block->set_offset(size);

      buildExtendedBlockProto(located_block->mutable_b(), block_id, block_size);

      auto data_nodes = std::vector<std::string>();
      std::string block_metadata_path = get_block_metadata_path(block_id);
      LOG(INFO) << "Getting datanode locations for block: "
                << block_metadata_path;

      if (!zk->get_children(block_metadata_path,
                            data_nodes, error_code)) {
        LOG(ERROR) << "Failed getting datanode locations for block: "
                   << block_metadata_path
                   << " with error: "
                   << error_code;
      }

      LOG(INFO) << "Found block locations " << data_nodes.size();

      auto sorted_data_nodes = std::vector<std::string>();
      if (sort_by_xmits(data_nodes, sorted_data_nodes)) {
        for (auto data_node = sorted_data_nodes.begin();
             data_node != sorted_data_nodes.end();
             ++data_node) {
          LOG(INFO) << "Block DN Loc: " << *data_node;
          buildDatanodeInfoProto(located_block->add_locs(), *data_node);
        }
      } else {
        LOG(ERROR)
            << "Unable to sort DNs by # xmits in get_block_locations. "
                "Using unsorted instead.";
        for (auto data_node = data_nodes.begin();
             data_node != data_nodes.end();
             ++data_node) {
          LOG(INFO) << "Block DN Loc: " << *data_node;
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
      LOG(ERROR) << "Failed to get " << dn_stats_path;
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
                             GetContentSummaryResponseProto &res) {
  const std::string &path = req.path();

  if (file_exists(path)) {
    LOG(INFO) << "File exists";
    // read the node into the file node struct
    FileZNode znode_data;
    read_file_znode(znode_data, path);

    // set the file status in the get file info response res
    ContentSummaryProto *status = res.mutable_summary();

    set_file_info_content(status, path, znode_data);
    LOG(INFO) << "Got info for file ";
    return;
  }
  LOG(INFO) << "No file to get info for";
  return;
}

void ZkNnClient::set_file_info_content(ContentSummaryProto *status,
                                       const std::string &path,
                                       FileZNode &znode_data) {
  // get the filetype, since we do not want to serialize an enum
  int error_code = 0;
  if (znode_data.filetype == IS_DIR) {
    int num_file = 0;
    int num_dir = 0;
    std::vector<std::string> children;
    if (!zk->get_children(ZookeeperBlocksPath(path), children, error_code)) {
      LOG(FATAL) << "Failed to get children for " << ZookeeperBlocksPath(path);
    } else {
      for (auto &child : children) {
        auto child_path = util::concat_path(path, child);
        FileZNode child_data;
        read_file_znode(child_data, child_path);
        if (child_data.filetype == IS_FILE) {
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

  LOG(INFO) << "Successfully set the file info ";
}

void ZkNnClient::set_file_info(HdfsFileStatusProto *status,
                               const std::string &path, FileZNode &znode_data) {
  HdfsFileStatusProto_FileType filetype;
  // get the filetype, since we do not want to serialize an enum
  switch (znode_data.filetype) {
    case (0):filetype = HdfsFileStatusProto::IS_DIR;
      break;
    case (1):filetype = HdfsFileStatusProto::IS_DIR;
      break;
    case (2):filetype = HdfsFileStatusProto::IS_FILE;
      break;
    default:break;
  }

  FsPermissionProto *permission = status->mutable_permission();
  // Shorcut to set permission to 777.
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
      ErasureCodingPolicyProto *ecPolicyProto = status->mutable_ecpolicy();
      ecPolicyProto->set_name(DEFAULT_EC_POLICY);
      // TODO(nate): check to see if the unit is expected to be in kb.
      ecPolicyProto->set_cellsize(DEFAULT_EC_CELLCIZE);
      ecPolicyProto->set_id(DEFAULT_EC_ID);

      ECSchemaProto* ecSchema = ecPolicyProto->mutable_schema();
      ecSchema->set_codecname(DEFAULT_EC_CODEC_NAME);
      ecSchema->set_dataunits(DEFAULT_DATA_UNITS);
      ecSchema->set_parityunits(DEFAULT_PARITY_UNITS);
      ecPolicyProto->set_allocated_schema(ecSchema);
      status->set_allocated_ecpolicy(ecPolicyProto);
  }

  LOG(INFO) << "Successfully set the file info ";
}

bool ZkNnClient::add_block(const std::string &file_path,
                           std::uint64_t &block_id,
                           std::vector<std::string> &data_nodes,
                           uint32_t replicationFactor) {
  if (!file_exists(file_path)) {
    LOG(ERROR) << "Cannot add block to non-existent file" << file_path;
    return false;
  }

  FileZNode znode_data;
  read_file_znode(znode_data, file_path);

  std::string block_id_str;

  util::generate_block_id(block_id);
  block_id_str = std::to_string(block_id);
  LOG(INFO) << "Generated block id " << block_id_str;

  if (!find_datanode_for_block(data_nodes, block_id, replicationFactor,
                               true, znode_data.blocksize)) {
    return false;
  }

  // Generate the massive multi-op for creating the block

  std::vector<std::uint8_t> data;
  data.resize(sizeof(u_int64_t));
  memcpy(&data[0], &block_id, sizeof(u_int64_t));

  LOG(INFO) << "Generating block for " << ZookeeperBlocksPath(file_path);

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
        << "Failed to write the addBlock multiop, ZK state was not changed";
    ZKWrapper::print_error(err);
    return false;
  }
  return true;
}

bool ZkNnClient::add_block_group(const std::string &fileName,
                     u_int64_t &block_group_id,
                     std::vector<std::string> &dataNodes,
                     std::vector<u_int64_t> &storageBlockIDs,
                     uint32_t total_num_storage_blocks) {
    FileZNode znode_data;
    read_file_znode(znode_data, fileName);

    // Generate the block group id and storage block ids.
    block_group_id = generate_block_group_id();
    for (u_int64_t i = 0; i < total_num_storage_blocks; i++) {
        storageBlockIDs.push_back(generate_storage_block_id(block_group_id, i));
    }

    // Log them for debugging purposes.
    LOG(INFO) << "Generated block group id " << block_group_id << "\n";
    for (auto storageBlockID : storageBlockIDs)
        LOG(INFO) << "Generated storage block id " << storageBlockID << "\n";

    // TODO(nate): figure out what exact zookeepr operations must occur.
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


// TODO(2016): To simplify signature, could just get rid of the newBlock param
// and always check for preexisting replicas
bool ZkNnClient::find_datanode_for_block(std::vector<std::string> &datanodes,
                                         const u_int64_t blockId,
                                         uint32_t replication_factor,
                                         bool newBlock,
                                         uint64_t blocksize) {
  // TODO(2016): Actually perform this action
  // TODO(2016): Perhaps we should keep a cached list of nodes

  std::vector<std::string> live_data_nodes = std::vector<std::string>();
  int error_code;
  std::string block_metadata_path = get_block_metadata_path(blockId);
  // Get all of the live datanodes
  if (zk->get_children(HEALTH, live_data_nodes, error_code)) {
    // LOG(INFO) << "Found live DNs: " << live_data_nodes;
    auto excluded_datanodes = std::vector<std::string>();
    if (!newBlock) {
      // Get the list of datanodes which already have a replica
      if (!zk->get_children(block_metadata_path,
                            excluded_datanodes, error_code)) {
        LOG(ERROR) << "Error getting children of: "
                   << block_metadata_path;
        return false;
      }
    }

    std::priority_queue<TargetDN> targets;
    /* for each child, check if the ephemeral node exists */
    for (auto datanode : live_data_nodes) {
      bool isAlive;
      if (!zk->exists(HEALTH_BACKSLASH + datanode + HEARTBEAT,
                      isAlive, error_code)) {
        LOG(ERROR) << "Failed to check if datanode: " + datanode
                   << " is alive: "
                   << error_code;
      }
      if (isAlive) {
        bool exclude = false;
        if (excluded_datanodes.size() > 0) {
          // Remove the excluded datanodes from the live list
          std::vector<std::string>::iterator it = std::find(
              excluded_datanodes.begin(),
              excluded_datanodes.end(),
              datanode);

          if (it == excluded_datanodes.end()) {
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
            LOG(ERROR) << "Failed to get " << dn_stats_path;
            return false;
          }
          DataNodePayload stats = DataNodePayload();
          memcpy(&stats, &stats_payload[0], sizeof(DataNodePayload));
          LOG(INFO) << "\t DN stats - free_bytes: "
                    << unsigned(stats.free_bytes);
          if (stats.free_bytes > blocksize) {
            auto queue = REPLICATE_QUEUES + datanode;
            auto repl_item = util::concat_path(queue, std::to_string(blockId));
            bool alreadyOnQueue;
            // do not put something on queue if its already on there
            if (zk->exists(repl_item, alreadyOnQueue, error_code)) {
              if (alreadyOnQueue) {
                LOG(INFO) << "Skipping target" << datanode;
                continue;
              }
            }
            LOG(INFO) << "Pushed target: "
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

    LOG(INFO) << "There are "
              << targets.size()
              << " viable DNs. Requested: "
              << replication_factor;
    if (targets.size() < replication_factor) {
      LOG(ERROR) << "Not enough available DNs! Available: "
                 << targets.size()
                 << " Requested: "
                 << replication_factor;
      // no return because we still want the client to write to some datanodes
    }

    while (datanodes.size() < replication_factor && targets.size() > 0) {
      LOG(INFO) << "DNs size IS : " << datanodes.size();
      TargetDN target = targets.top();
      datanodes.push_back(target.dn_id);
      LOG(INFO) << "Selecting target DN "
                << target.dn_id
                << " with "
                << target.num_xmits
                << " xmits and "
                << target.free_bytes
                << " free bytes";
      targets.pop();
    }

  } else {
    LOG(ERROR) << "Failed to get list of datanodes at "
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
    LOG(ERROR) << "Failed to find at least "
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
  std::string dst_znode = ZookeeperFilePath(dst);
  std::string dst_znode_blocks = ZookeeperBlocksPath(dst);

  // Get the payload from the old filesystem znode for the src
  zk->get(src_znode, data, error_code);
  if (error_code != ZOK) {
    LOG(ERROR) << "Failed to get data from '"
               << src_znode
               << "' when renaming.";
    return false;
  }

  // Create a new znode in the filesystem for the dst
  LOG(INFO) << "Added op#" << ops.size() << ": create " << dst_znode;
  ops.push_back(zk->build_create_op(dst_znode, data));

    zk->get(src_znode_blocks, data, error_code);
    if (error_code != ZOK) {
        LOG(ERROR) << "Failed to get data from '"
                   << src_znode
                   << "' when renaming.";
        return false;
    }

    // Create a new znode in the filesystem for the dst
    LOG(INFO) << "Added op#" << ops.size() << ": create " << dst_znode_blocks;
    ops.push_back(zk->build_create_op(dst_znode_blocks, data));

  // Copy over the data from the children of the src_znode
  // into new children of the dst_znode
  auto children = std::vector<std::string>();
  zk->get_children(src_znode_blocks, children, error_code);
  if (error_code != ZOK) {
    LOG(ERROR) << "Failed to get children of znode '"
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
    LOG(INFO) << "Added op#"
              << ops.size()
              << ": create "
              << dst_znode + "/" + child;
    ops.push_back(zk->build_create_op(dst_znode_blocks + "/" + child,
                                      child_data));

    // Delete src_znode's child
    LOG(INFO) << "Added op#"
              << ops.size()
              << ": delete "
              << src_znode + "/" + child;
    ops.push_back(zk->build_delete_op(src_znode_blocks + "/" + child));
  }

  // Remove the old znode for the src
  LOG(INFO) << "Added op#" << ops.size() << ": delete " << src_znode_blocks;
  ops.push_back(zk->build_delete_op(src_znode_blocks));
  LOG(INFO) << "Added op#" << ops.size() << ": delete " << src_znode;

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
    LOG(ERROR) << "Failed to get data from '"
               << src_znode
               << "' when renaming.";
    return false;
  }

  // Create a new znode in the filesystem for the dst
  LOG(INFO) << "rename_ops_for_dir - Added op#"
            << ops.size()
            << ": create "
            << dst_znode;
  ops.push_back(zk->build_create_op(dst_znode, data));

  // Iterate through the items in this dir
  auto children = std::vector<std::string>();
  if (!zk->get_children(src_znode, children, error_code)) {
    LOG(ERROR) << "Failed to get children of znode '"
               << src_znode
               << "' when renaming.";
    return false;
  }

  auto nested_dirs = std::vector<std::string>();
  for (auto child : children) {
    std::string child_path = src + "/" + child;
    FileZNode znode_data;
    read_file_znode(znode_data, child_path);
    if (znode_data.filetype == IS_DIR) {
      LOG(INFO) << "Child: " << child << " is DIR";
      // Keep track of any nested directories
      nested_dirs.push_back(child);
    } else if (znode_data.filetype == IS_FILE) {
      // Generate ops for each file in the dir
      LOG(INFO) << "Child: " << child << " is FILE";
      if (!rename_ops_for_file(src + "/" + child, dst + "/" + child, ops)) {
        return false;
      }
    } else {
      LOG(ERROR) << "Requested rename source: "
                 << child_path
                 << " is not a file or dir";
      return false;
    }
  }

  // Iterate through the found nested directories and generate ops for them
  LOG(INFO) << "Found " << nested_dirs.size() << " nested dirs";
  for (auto dir : nested_dirs) {
    LOG(INFO) << "Call recursively on: " << src + "/" + dir;
    if (!rename_ops_for_dir(src + "/" + dir, dst + "/" + dir, ops)) {
      return false;
    }
  }
  // Delete the old dir
  LOG(INFO) << "Added op#" << ops.size() << ": delete " << src_znode;
  ops.push_back(zk->build_delete_op(src_znode));

  return true;
}

/**
 * Checks that each block UUID in the wait_for_acks dir:
 *	 1. has REPLICATION_FACTOR many children
 *	 2. if the block UUID was created more than ACK_TIMEOUT milliseconds ago
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
    LOG(ERROR) << "ERROR CODE: "
               << error_code
               << " occurred in check_acks when getting children for "
               << WORK_QUEUES + std::string(WAIT_FOR_ACK);
    return false;  // TODO(2016): Is this the right return val?
  }
  LOG(INFO) << "Checking acks for: " << block_uuids.size() << " blocks";

  for (auto block_uuid : block_uuids) {
    uint64_t block_id;
    std::stringstream strm(block_uuid);
    strm >> block_id;
    bool ec = ZkClientCommon::is_ec_block(block_id);
    LOG(INFO) << "Considering block: " << block_uuid;
    std::string block_path = WORK_QUEUES + std::string(WAIT_FOR_ACK_BACKSLASH)
        + block_uuid;

    auto data = std::vector<std::uint8_t>();
    if (!zk->get(block_path, data, error_code)) {
      LOG(ERROR) << "Error getting payload at: " << block_path;
      return false;
    }
    int replication_factor = unsigned(data[0]);
    LOG(INFO) << "Replication factor for "
              << block_uuid
              << " is "
              << replication_factor;

    // Get the datanodes with have replicated this block
    auto existing_dn_replicas = std::vector<std::string>();
    if (!zk->get_children(block_path, existing_dn_replicas, error_code)) {
      LOG(ERROR) << "ERROR CODE: "
                 << error_code
                 << " occurred in check_acks when getting children for "
                 << block_path;
      return false;
    }
    LOG(INFO) << "Found "
              << existing_dn_replicas.size()
              << " replicas of "
              << block_uuid;

    int elapsed_time = ms_since_creation(block_path);
    if (elapsed_time < 0) {
      LOG(ERROR) << "Failed to get elapsed time";
    }

    if (existing_dn_replicas.size() == 0 && elapsed_time > ACK_TIMEOUT && !ec) {
      // Block is not available on any DNs and cannot be replicated.
      // Emit error and remove this block from wait_for_acks
      LOG(ERROR) << block_path << " has 0 replicas! Delete from wait_for_acks.";
      if (!zk->delete_node(block_path, error_code)) {
        LOG(ERROR) << "Failed to delete: " << block_path;
        return false;
      }
      return false;

    } else if (existing_dn_replicas.size() < replication_factor
        && elapsed_time > ACK_TIMEOUT) {
      LOG(INFO) << "Not yet enough replicas after time out for " << block_uuid;
      // Block hasn't been replicated enough, request remaining replicas
      int replicas_needed = replication_factor - existing_dn_replicas.size();
      LOG(INFO) << replicas_needed << " replicas are needed";

      std::vector<std::string> to_replicate;
      for (int i = 0; i < replicas_needed; i++) {
        to_replicate.push_back(block_uuid);
      }
      if (ec) {
        if (!recover_ec_blocks(to_replicate, error_code)) {
          LOG(ERROR) << "Failed to add necessary items to ec_recover queue.";
          return false;
        } else {
          LOG(INFO) << "Created "
                    << to_replicate.size()
                    << " items in the ec_recover queue.";
        }
      } else {
        if (!replicate_blocks(to_replicate, error_code)) {
          LOG(ERROR) << "Failed to add necessary items to replication queue.";
          return false;
        } else {
          LOG(INFO) << "Created "
                    << to_replicate.size()
                    << " items in the replication queue.";
        }
      }

    } else if (existing_dn_replicas.size() == replication_factor) {
      LOG(INFO) << "Enough replicas have been made, no longer need to wait on "
                << block_path;
      if (!zk->recursive_delete(block_path, error_code)) {
        LOG(ERROR) << "Failed to delete: " << block_path;
        return false;
      }
    } else {
      LOG(INFO) << "Not enough replicas, but still time left" << block_path;
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
      LOG(ERROR) << "Replicate could not read the block size for block: "
                 << block_id;
      return false;
    }
    if (!find_datanode_for_block(target_dn, block_id, 1, false, blocksize)
        || target_dn.size() == 0) {
      LOG(ERROR) << " Failed to find datanode for this block! " << rec;
      return false;
    }
    auto queue = EC_RECOVER_QUEUES + target_dn[0];
    auto rec_item = util::concat_path(queue, rec);
    ops.push_back(zk->build_create_op(rec_item, ZKWrapper::EMPTY_VECTOR));
  }

  // We do not need to sync this multi-op immediately
  if (!zk->execute_multi(ops, results, err, false)) {
    LOG(ERROR) << "Failed to execute multiop for recover_ec_blocks";
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
      LOG(ERROR) << "Replicate could not read the block size for block: "
                 << block_id;
      return false;
    }
    if (!find_datanode_for_block(target_dn, block_id, 1, false, blocksize)
        || target_dn.size() == 0) {
      LOG(ERROR) << " Failed to find datanode for this block! " << repl;
      return false;
    }
    auto queue = REPLICATE_QUEUES + target_dn[0];
    auto repl_item = util::concat_path(queue, repl);
    ops.push_back(zk->build_create_op(repl_item, ZKWrapper::EMPTY_VECTOR));
  }

  // We do not need to sync this multi-op immediately
  if (!zk->execute_multi(ops, results, err, false)) {
    LOG(ERROR) << "Failed to execute multiop for replicate_blocks";
    return false;
  }

  return true;
}

bool ZkNnClient::find_all_datanodes_with_block(
    const uint64_t &block_uuid,
    std::vector<std::string> &rdatanodes, int &error_code) {
  std::string block_loc_path = get_block_metadata_path(block_uuid);

  if (!zk->get_children(block_loc_path, rdatanodes, error_code)) {
    LOG(ERROR) << "Failed to get children of: " << block_loc_path;
    return false;
  }
  if (rdatanodes.size() < 1) {
    LOG(ERROR) << "There are no datanodes with a replica of block "
               << block_uuid;
    return false;
  }
  return true;
}

int ZkNnClient::ms_since_creation(std::string &path) {
  int error;
  struct Stat stat;
  if (!zk->get_info(path, stat, error)) {
    LOG(ERROR) << "Failed to get info for: " << path;
    return -1;
  }
  LOG(INFO) << "Creation time of " << path << " was: "
            << stat.ctime << " ms ago";
  uint64_t current_time = current_time_ms();
  LOG(INFO) << "Current time is: " << current_time << "ms";
  int elapsed = current_time - stat.ctime;
  LOG(INFO) << "Elapsed ms: " << elapsed;
  return elapsed;
}

/**
 * Returns the current timestamp in milliseconds
 */
uint64_t ZkNnClient::current_time_ms() {
  // http://stackoverflow.com/questions/19555121/how-to-get-current-timestamp-in-milliseconds-since-1970-just-the-way-java-gets
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
    LOG(ERROR) << "Getting data node stats failed with " << error_code;
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
}  // namespace zkclient

#endif
