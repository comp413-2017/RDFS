// Copyright 2017 Rice University, COMP 413 2017

#ifndef RDFS_ZK_CLIENT_DN_CC
#define RDFS_ZK_CLIENT_DN_CC

#include "zk_dn_client.h"
#include "zk_lock.h"
#include <easylogging++.h>
#include <google/protobuf/message.h>
#include "hdfs.pb.h"
#include "util.h"
#include "data_transfer_server.h"

namespace zkclient {

ZkClientDn::ZkClientDn(const std::string &ip, std::shared_ptr<ZKWrapper> zk_in,
                       uint64_t total_disk_space, const uint32_t ipcPort,
                       const uint32_t xferPort) : ZkClientCommon(
    zk_in) {

  registerDataNode(ip, total_disk_space, ipcPort, xferPort);
}

ZkClientDn::ZkClientDn(const std::string &ip, const std::string &zkIpAndAddress,
                       uint64_t total_disk_space, const uint32_t ipcPort,
                       const uint32_t xferPort) : ZkClientCommon(
    zkIpAndAddress) {

  registerDataNode(ip, total_disk_space, ipcPort, xferPort);
}

bool ZkClientDn::blockReceived(uint64_t uuid, uint64_t size_bytes) {
  int error_code;
  bool exists;
  bool created_correctly = true;

  LOG(INFO) << "DataNode received a block with UUID " << std::to_string(uuid);
  std::string id = build_datanode_id(data_node_id);

  // We are performing acks right now
  /*
  ZKLock queue_lock(*zk.get(), WORK_QUEUES + WAIT_FOR_ACK_BACKSLASH
   + std::to_string(uuid));
  if (queue_lock.lock() != 0) {
      LOG(ERROR)
      <<  "Failed locking on /work_queues/wait_for_acks/<block_uuid> "
      << error_code;
      created_correctly = false;
  }

  if (zk->exists(WORK_QUEUES + WAIT_FOR_ACK_BACKSLASH + std::to_string(uuid),
   exists, error_code)) {
      if (!exists) {
          LOG(ERROR)
          << WORK_QUEUES + WAIT_FOR_ACK_BACKSLASH + std::to_string(uuid)
          << " does not exist";
          created_correctly = false;
      }
      if(!zk->create(WORK_QUEUES + WAIT_FOR_ACK_BACKSLASH +
      std::to_string(uuid) + "/" + id, ZKWrapper::EMPTY_VECTOR,
      error_code, true, false)) {
          LOG(ERROR)
          <<  "Failed to create wait_for_acks/<block_uuid>/datanode_id "
          << error_code;
          created_correctly = false;
      }
  }

  if (queue_lock.unlock() != 0) {
      LOG(ERROR)
      <<  "Failed unlocking on /work_queues/wait_for_acks/<block_uuid> "
      << error_code;
      created_correctly = false;
  }
  */

  if (zk->exists(BLOCK_LOCATIONS + std::to_string(uuid), exists, error_code)) {
    // If the block_location does not yet exist. Flush its path.
    // If it still does not exist error out.
    if (!exists) {
      zk->flush(zk->prepend_zk_root(BLOCK_LOCATIONS + std::to_string(uuid)));
      if (zk->exists(BLOCK_LOCATIONS + std::to_string(uuid),
                     exists, error_code)) {
        LOG(ERROR) << "/block_locations/<block_uuid> did not exist "
                   << error_code;
        return false;
      }
    }
    // Write the block size
    BlockZNode block_data;
    block_data.block_size = size_bytes;
    std::vector<std::uint8_t> data_vect(sizeof(block_data));
    memcpy(&data_vect[0], &block_data, sizeof(block_data));
    if (!zk->set(BLOCK_LOCATIONS + std::to_string(uuid),
                 data_vect, error_code, false)) {
      LOG(ERROR)
              << "Failed writing block size to /block_locations/<block_uuid> "
              << error_code;
      created_correctly = false;
    }

    // We do not need to sync these operations immediately
    // Add this datanode as the block's location in block_locations
    if (!zk->create(BLOCK_LOCATIONS + std::to_string(uuid) +
                            "/" + id,
                    ZKWrapper::EMPTY_VECTOR,
                    error_code,
                    true,
                    false)) {
      LOG(ERROR)
              << "Failed creating /block_locations/<block_uuid>/<datanode_id> "
              << error_code;
      created_correctly = false;
    }
  }

  // Write block to /blocks
  if (zk->exists(HEALTH_BACKSLASH + id + BLOCKS, exists, error_code)) {
    if (exists) {
      // We do not need to sync these operations immediately
      if (!zk->create(HEALTH_BACKSLASH + id + BLOCKS +
                              "/" + std::to_string(uuid),
                      ZKWrapper::EMPTY_VECTOR,
                      error_code,
                      false,
                      false)) {
        LOG(ERROR)
                << "Failed creating /health/<data_node_id>/blocks/<block_uuid> "
                << error_code;
      }
    }
  }

  return created_correctly;
}

void ZkClientDn::registerDataNode(const std::string &ip,
                                  uint64_t total_disk_space,
                                  const uint32_t ipcPort,
                                  const uint32_t xferPort) {
  // TODO(2016): Consider using startup time of the DN
  // along with the ip and port
  int error_code;
  bool exists;

  data_node_id = DataNodeId();
  data_node_id.ip = ip;
  data_node_id.ipcPort = ipcPort;

  data_node_payload = DataNodePayload();
  data_node_payload.ipcPort = ipcPort;
  data_node_payload.xferPort = xferPort;
  data_node_payload.disk_bytes = total_disk_space;
  data_node_payload.free_bytes = total_disk_space;
  data_node_payload.xmits = 0;

  std::string id = build_datanode_id(data_node_id);
  // TODO(2016): Add a watcher on the health node
  if (zk->exists(HEALTH_BACKSLASH + id, exists, error_code)) {
    if (exists) {
      if (!zk->recursive_delete(HEALTH_BACKSLASH + id, error_code)) {
        LOG(ERROR) << "Failed deleting /health/<data_node_id> " << error_code;
      }
    }
  }

  if (!zk->create(HEALTH_BACKSLASH + id,
                  ZKWrapper::EMPTY_VECTOR, error_code, false)) {
    LOG(ERROR) << "Failed creating /health/<data_node_id> "
               << error_code;
  }


  // Create an ephemeral node at /health/<datanode_id>/heartbeat
  // if it doesn't already exist. Should have a ZOPERATIONTIMEOUT
  if (!zk->create(HEALTH_BACKSLASH + id + HEARTBEAT,
                  ZKWrapper::EMPTY_VECTOR, error_code, true)) {
    LOG(ERROR) << "Failed creating /health/<data_node_id>/heartbeat "
               << error_code;
  }

  std::vector<uint8_t> data;
  data.resize(sizeof(DataNodePayload));
  memcpy(&data[0], &data_node_payload, sizeof(DataNodePayload));

  if (!zk->create(HEALTH_BACKSLASH + id + STATS, data, error_code, true)) {
    LOG(ERROR) << "Failed creating /health/<data_node_id>/stats "
               << error_code;
  }

  if (!zk->create(HEALTH_BACKSLASH + id + BLOCKS,
                  ZKWrapper::EMPTY_VECTOR, error_code, false)) {
    LOG(ERROR) << "Failed creating /health/<data_node_id>/blocks "
               << error_code;
  }

  // Create the delete queue
  if (zk->exists(DELETE_QUEUES + id, exists, error_code)) {
    if (!exists) {
      LOG(INFO) << "doesn't exist, trying to make it";
      if (!zk->create(DELETE_QUEUES + id, ZKWrapper::EMPTY_VECTOR,
                      error_code, false)) {
        LOG(INFO) << "delete queue Creation failed";
      }
    }
  }
  // Create the replicate queue
  if (zk->exists(REPLICATE_QUEUES + id, exists, error_code)) {
    if (!exists) {
      LOG(INFO) << "doesn't exist, trying to make it";
      if (!zk->create(REPLICATE_QUEUES + id, ZKWrapper::EMPTY_VECTOR,
                      error_code, false)) {
        LOG(INFO) << "replicate queue Creation failed";
      }
    }
  }
  LOG(INFO) << "Registered datanode " + build_datanode_id(data_node_id);
}

bool ZkClientDn::push_dn_on_repq(std::string dn_name, uint64_t blockid) {
  auto queue_path = util::concat_path(ZkClientCommon::REPLICATE_QUEUES,
                                      dn_name);
  auto my_id = build_datanode_id(data_node_id);
  LOG(INFO) << "adding datanode and block to replication queue "
            << dn_name
            << " "
            << std::to_string(blockid);
  std::vector<uint8_t> block_vec(sizeof(uint64_t));
  memcpy(&block_vec[0], &blockid, sizeof(uint64_t));
  int error;
  std::vector<std::shared_ptr<ZooOp>> ops;
  std::vector<zoo_op_result> results;
  auto work_item = util::concat_path(queue_path, std::to_string(blockid));
  if (!zk->create(work_item, ZKWrapper::EMPTY_VECTOR, error, false)) {
    LOG(ERROR) << "Failed to create replication commands for pipelining!";
    return false;
  }
  return true;
}

void ZkClientDn::setTransferServer(std::shared_ptr<TransferServer> &server) {
  this->server = server;
}

bool ZkClientDn::poll_replication_queue() {
  // LOG(INFO) << " poll replication queue";
  handleReplicateCmds(util::concat_path(REPLICATE_QUEUES, get_datanode_id()));
  return true;
}

bool ZkClientDn::poll_delete_queue() {
  LOG(INFO) << " poll delete queue";
  processDeleteQueue();
  return true;
}

void ZkClientDn::handleReplicateCmds(const std::string &path) {
  int err;
  // LOG(ERROR) << "handling replicate watcher for " << path;
  std::vector<std::string> work_items;

  if (!zk->get_children(path, work_items, err)) {
    LOG(ERROR) << "Failed to get work items!";
    return;
  }

  if (work_items.size() > 0) {
    LOG(INFO) << get_datanode_id()
              << "FOUND "
              << work_items.size()
              << " work items";
  }

  std::vector<std::shared_ptr<ZooOp>> ops;
  for (auto &block : work_items) {
    auto full_work_item_path = util::concat_path(path, block);

    std::string read_from;
    // get block size
    std::vector<std::uint8_t> block_size_vec(sizeof(std::uint64_t));
    std::uint64_t block_size;
    if (!zk->get(util::concat_path(BLOCK_LOCATIONS, block), block_size_vec,
                 err, sizeof(std::uint64_t))) {
      LOG(ERROR) << "could not get the block length for "
                 << block
                 << " because of "
                 << err;
      continue;
    }
    memcpy(&block_size, &block_size_vec[0], sizeof(uint64_t));

    // build block proto
    hadoop::hdfs::ExtendedBlockProto block_proto;
    std::uint64_t block_id;
    std::stringstream strm(block);
    strm >> block_id;
    LOG(INFO) << "Block id is " << std::to_string(block_id) << " " << block;
    buildExtendedBlockProto(&block_proto, block_id, block_size);

    bool delete_item = false;
    if (!find_datanode_with_block(std::to_string(block_id), read_from, err)) {
      LOG(ERROR) << "Could not find datanode with block, going to delete";
      delete_item = true;  // delete it
    } else {
      std::vector<std::string> split_address;
      boost::split(split_address, read_from, boost::is_any_of(":"));
      assert(split_address.size() == 2);
      // get the port
      std::string dn_ip = split_address[0];
      DataNodePayload dn_target_info;
      std::vector<std::uint8_t> dn_data(sizeof(DataNodePayload));
      if (!zk->get(HEALTH_BACKSLASH + read_from + STATS, dn_data, err,
                   sizeof(DataNodePayload))) {
        LOG(ERROR) << "failed to read target dn payload" << err;
        continue;
      }
      memcpy(&dn_target_info, &dn_data[0], sizeof(DataNodePayload));
      std::uint32_t xferPort = dn_target_info.xferPort;
      // actually do the inter datanode communication, try twice(short circuit)
      if (server->replicate(block_size, dn_ip,
                            std::to_string(xferPort), block_proto)) {
        LOG(INFO) << "Replication successful.";
        delete_item = true;
      } else {
        LOG(ERROR) << "Replication unsuccessful. Gonna try again later";
      }
    }
    if (delete_item) {
      ops.push_back(zk->build_delete_op(full_work_item_path));
    }
  }
  // delete work items
  std::vector<zoo_op_result> results;
  if (!zk->execute_multi(ops, results, err)) {
    LOG(ERROR)
            << "Failed to delete successfully completed replicate commands!";
  }
}

void ZkClientDn::processDeleteQueue() {
  std::string path = util::concat_path(DELETE_QUEUES, get_datanode_id());
  int error_code;
  bool exists;
  int err;
  std::string id = build_datanode_id(data_node_id);
  uint64_t block_id;
  std::vector<std::string> work_items;
  std::vector<std::shared_ptr<ZooOp>> ops;

  if (!zk->get_children(path, work_items, err)) {
    LOG(ERROR) << "Failed to get work items!";
    return;
  }

  LOG(INFO) << "Deleting this many blocks " << work_items.size();

  for (auto &block : work_items) {
    LOG(INFO) << "Delete working on " << block;
    // get block id
    std::vector<std::uint8_t> block_id_vec(sizeof(std::uint64_t));
    std::uint64_t block_id;
    if (!zk->get(util::concat_path(path, block), block_id_vec, err,
                 sizeof(std::uint64_t))) {
      LOG(ERROR) << "could not get the block id "
                 << block
                 << " because of "
                 << err;
      return;
    }
    memcpy(&block_id, &block_id_vec[0], sizeof(uint64_t));
    if (!server->rmBlock(block_id)) {
      LOG(ERROR) << "Block is not in fs " << block;
    } else {
      // just delete the thing from the queue
      LOG(INFO) << "successful delete " << block;
    }
    ops.push_back(zk->build_delete_op(util::concat_path(path, block)));
  }
  std::vector<zoo_op_result> results;
  if (!zk->execute_multi(ops, results, err)) {
    LOG(ERROR) << "Failed to delete sucessfully completed delete commands!";
  }
}

ZkClientDn::~ZkClientDn() {
  zk->close();
}

std::string ZkClientDn::build_datanode_id(DataNodeId data_node_id) {
  return data_node_id.ip + ":" + std::to_string(data_node_id.ipcPort);
}

std::string ZkClientDn::get_datanode_id() {
  return build_datanode_id(this->data_node_id);
}

bool ZkClientDn::sendStats(uint64_t free_space, uint32_t xmits) {
  int error_code;

  std::string id = build_datanode_id(data_node_id);
  data_node_payload.free_bytes = free_space;
  data_node_payload.xmits = xmits;
  std::vector<uint8_t> data;
  data.resize(sizeof(DataNodePayload));
  memcpy(&data[0], &data_node_payload, sizeof(DataNodePayload));
  if (!zk->set(HEALTH_BACKSLASH + id + STATS, data, error_code, false)) {
    LOG(ERROR) << "Failed setting /health/<data_node_id>/stats with error "
               << error_code;
    return false;
  }
  return true;
}

bool ZkClientDn::buildExtendedBlockProto(hadoop::hdfs::ExtendedBlockProto *eb,
                                         const std::uint64_t &block_id,
                                         const uint64_t &block_size) {
  eb->set_poolid("0");
  eb->set_blockid(block_id);
  eb->set_generationstamp(1);
  eb->set_numbytes(block_size);
  return true;
}

bool ZkClientDn::find_datanode_with_block(const std::string &block_uuid_str,
                                          std::string &datanode,
                                          int &error_code) {
  std::vector<std::string> datanodes;
  std::string block_loc_path = BLOCK_LOCATIONS + block_uuid_str;

  if (!zk->get_children(block_loc_path, datanodes, error_code)) {
    LOG(ERROR) << "Failed to get children of: " << block_loc_path;
    return false;
  }
  if (datanodes.size() < 1) {
    LOG(ERROR) << "There are no datanodes with a replica of block "
               << block_uuid_str;
    // TODO(2016): set error_code
    return false;
  }
  // TODO(2016): Pick the datanode which has fewer transmits

  datanode = datanodes[0];
  LOG(INFO) << "Found DN: "
            << datanode
            << " which has a replica of: "
            << block_uuid_str;
  return true;
}
}  // namespace zkclient
#endif  // RDFS_ZK_CLIENT_DN_H
