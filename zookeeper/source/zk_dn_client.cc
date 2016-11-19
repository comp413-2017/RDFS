#ifndef RDFS_ZK_CLIENT_DN_CC
#define RDFS_ZK_CLIENT_DN_CC

#include "zk_dn_client.h"
#include "zk_lock.h"
#include "zk_queue.h"
#include <easylogging++.h>
#include <google/protobuf/message.h>
#include "hdfs.pb.h"
#include "util.h"
#include "data_transfer_server.h"

namespace zkclient{

    using namespace hadoop::hdfs;

	const std::string ZkClientDn::CLASS_NAME = ": **ZkClientDn** : ";

	ZkClientDn::ZkClientDn(const std::string& ip, std::shared_ptr <ZKWrapper> zk_in, uint64_t total_disk_space,
		const uint32_t ipcPort, const uint32_t xferPort) : ZkClientCommon(zk_in) {

		registerDataNode(ip, total_disk_space, ipcPort, xferPort);
	}

	ZkClientDn::ZkClientDn(const std::string& ip, const std::string& zkIpAndAddress, uint64_t total_disk_space,
		const uint32_t ipcPort, const uint32_t xferPort) : ZkClientCommon(zkIpAndAddress) {

		registerDataNode(ip, total_disk_space, ipcPort, xferPort);
	}

	bool ZkClientDn::blockReceived(uint64_t uuid, uint64_t size_bytes) {
		int error_code;
		bool exists;
		bool created_correctly = true;

		LOG(INFO) << "DataNode received a block with UUID " << std::to_string(uuid);
		std::string id = build_datanode_id(data_node_id);

		// Add acknowledgement
		ZKLock queue_lock(*zk.get(), WORK_QUEUES + WAIT_FOR_ACK_BACKSLASH + std::to_string(uuid));
		if (queue_lock.lock() != 0) {
			LOG(ERROR) << CLASS_NAME <<  "Failed locking on /work_queues/wait_for_acks/<block_uuid> " << error_code;
			created_correctly = false;
		}

		if (zk->exists(WORK_QUEUES + WAIT_FOR_ACK_BACKSLASH + std::to_string(uuid), exists, error_code)) {
			if (!exists) {
				LOG(ERROR) << CLASS_NAME << WORK_QUEUES + WAIT_FOR_ACK_BACKSLASH + std::to_string(uuid) << " does not exist";
				created_correctly = false;
			}
			if(!zk->create(WORK_QUEUES + WAIT_FOR_ACK_BACKSLASH + std::to_string(uuid) + "/" + id, ZKWrapper::EMPTY_VECTOR, error_code, false)) {
				LOG(ERROR) << CLASS_NAME <<  "Failed to create wait_for_acks/<block_uuid>/datanode_id " << error_code;
				created_correctly = false;
			}
		}

		if (queue_lock.unlock() != 0) {
			LOG(ERROR) << CLASS_NAME <<  "Failed unlocking on /work_queues/wait_for_acks/<block_uuid> " << error_code;
			created_correctly = false;
		}

		if (zk->exists(BLOCK_LOCATIONS + std::to_string(uuid), exists, error_code)) {
			if (exists) {
				// Write the block size
				BlockZNode block_data;
				block_data.block_size = size_bytes;
				std::vector<std::uint8_t> data_vect(sizeof(block_data));
				memcpy(&data_vect[0], &block_data, sizeof(block_data));
				if(!zk->set(BLOCK_LOCATIONS + std::to_string(uuid), data_vect, error_code)) {
					LOG(ERROR) << CLASS_NAME <<  "Failed writing block size to /block_locations/<block_uuid> " << error_code;
					created_correctly = false;
				}

				// Add this datanode as the block's location in block_locations
				if(!zk->create(BLOCK_LOCATIONS + std::to_string(uuid) + "/" + id, ZKWrapper::EMPTY_VECTOR, error_code, false)) {
					LOG(ERROR) << CLASS_NAME <<  "Failed creating /block_locations/<block_uuid>/<datanode_id> " << error_code;
					created_correctly = false;
				}
			}
			else {
				LOG(ERROR) << CLASS_NAME << "/block_locations/<block_uuid> did not exist " << error_code;
				return false;
			}
		}

		// Write block to /blocks
		if (zk->exists(HEALTH_BACKSLASH + id + BLOCKS, exists, error_code)) {
			if (exists) {
				if (!zk->create(HEALTH_BACKSLASH + id + BLOCKS + "/" + std::to_string(uuid), ZKWrapper::EMPTY_VECTOR, error_code)) {
					LOG(ERROR) << CLASS_NAME <<  "Failed creating /health/<data_node_id>/blocks/<block_uuid> " << error_code;
				}
			}
		}

		return created_correctly;
	}

	bool ZkClientDn::blockDeleted(uint64_t uuid) {
		int error_code;
		bool exists;

		LOG(INFO) << "DataNode deleted a block with UUID " << std::to_string(uuid);
		std::string id = build_datanode_id(data_node_id);

		auto ops = std::vector<std::shared_ptr<ZooOp>>();

		// Delete block locations
		if (zk->exists(BLOCK_LOCATIONS + std::to_string(uuid) + "/" + id, exists, error_code)) {
			if (exists) {
				ops.push_back(zk->build_delete_op(BLOCK_LOCATIONS + std::to_string(uuid) + "/" + id));
				// If deleted last child of block locations, delete block locations
				std::vector <std::string> children = std::vector <std::string>();
				if(!zk->get_children(BLOCK_LOCATIONS + std::to_string(uuid), children, error_code)){
					LOG(ERROR) << "getting children failed";
				}
				if (children.size() == 1) {
					ops.push_back(zk->build_delete_op(BLOCK_LOCATIONS + std::to_string(uuid)));
				}
			}
		}

		// Delete blocks
		if (zk->exists(HEALTH_BACKSLASH + id + BLOCKS + "/" + std::to_string(uuid), exists, error_code)) {
			if (exists) {
				ops.push_back(zk->build_delete_op(HEALTH_BACKSLASH + id + BLOCKS + "/" + std::to_string(uuid)));
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
	}

	void ZkClientDn::registerDataNode(const std::string& ip, uint64_t total_disk_space, const uint32_t ipcPort, const uint32_t xferPort) {
		// TODO: Consider using startup time of the DN along with the ip and port
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
		// TODO: Add a watcher on the health node
		if (zk->exists(HEALTH_BACKSLASH + id, exists, error_code)) {
			if (exists) {
				if (!zk->recursive_delete(HEALTH_BACKSLASH + id, error_code)) {
					LOG(ERROR) << CLASS_NAME <<  "Failed deleting /health/<data_node_id> " << error_code;
				}
			}
		}

		if (!zk->create(HEALTH_BACKSLASH + id, ZKWrapper::EMPTY_VECTOR, error_code)) {
			LOG(ERROR) << CLASS_NAME <<  "Failed creating /health/<data_node_id> " << error_code;
		}


		// Create an ephemeral node at /health/<datanode_id>/heartbeat
		// if it doesn't already exist. Should have a ZOPERATIONTIMEOUT
		if (!zk->create(HEALTH_BACKSLASH + id + HEARTBEAT, ZKWrapper::EMPTY_VECTOR, error_code, true)) {
			LOG(ERROR) << CLASS_NAME <<  "Failed creating /health/<data_node_id>/heartbeat " << error_code;
		}

		std::vector<uint8_t> data;
		data.resize(sizeof(DataNodePayload));
		memcpy(&data[0], &data_node_payload, sizeof(DataNodePayload));

		if (!zk->create(HEALTH_BACKSLASH + id + STATS, data, error_code, false)) {
			LOG(ERROR) << CLASS_NAME <<  "Failed creating /health/<data_node_id>/stats " << error_code;
		}

		if (!zk->create(HEALTH_BACKSLASH + id + BLOCKS, ZKWrapper::EMPTY_VECTOR, error_code)) {
			LOG(ERROR) << CLASS_NAME <<  "Failed creating /health/<data_node_id>/blocks " << error_code;
		}

		// Create the work queues, set their watchers
		ZkClientDn::initWorkQueue(REPLICATE_QUEUES, ZkClientDn::thisDNReplicationQueueWatcher, id);
		ZkClientDn::initWorkQueue(DELETE_QUEUES, ZkClientDn::thisDNDeleteQueueWatcher, id);
		LOG(INFO) << "Registered datanode " + build_datanode_id(data_node_id);
	}

	void ZkClientDn::initWorkQueue(std::string queueName, void (* watchFuncPtr)(zhandle_t *, int, int, const char *, void *), std::string id){
		int error_code;
		bool exists;

		// Create queue for this datanode
		// TODO: Replace w/ actual queues when they're created
		if (zk->exists(queueName + id, exists, error_code)){
				if (!exists){
						LOG(INFO) << "doesn't exist, trying to make it";
						if (!zk->create(queueName + id, ZKWrapper::EMPTY_VECTOR, error_code, false)){
								LOG(INFO) << "Creation failed";
						}
				}
		}

		// Register the replication watcher for this dn
		std::vector <std::string> children = std::vector <std::string>();
		if(!zk->wget_children(queueName + id, children, watchFuncPtr, this, error_code)){
			LOG(INFO) << "getting children failed";
		}

	}

	bool ZkClientDn::push_dn_on_repq(std::string dn_name, uint64_t blockid) {
		LOG(INFO) << "adding datanode and block to replication queue " << std::to_string(blockid);
		std::string queue_path = ZkClientCommon::REPLICATE_QUEUES + dn_name;
		std::string my_id = build_datanode_id(data_node_id);
		std::vector<uint8_t> data (sizeof(uint64_t));
		memcpy(&data[0], &blockid, sizeof(uint64_t));
		std::string pushed_path;
		int error;
		if (!zkqueue::push(zk, queue_path, data, pushed_path, error)) {
			LOG(ERROR) << " could not add replica to queue";
			return false;
		}
		zk->get(queue_path, data, error);
		memcpy(&blockid, &data[0], sizeof(uint64_t));
		LOG(INFO) << "adding datanode and block to replication queue " << std::to_string(blockid);
		LOG(INFO) << "writing " << my_id << " as a child on queue";
		if (!zk->create(pushed_path + "/" + my_id, ZKWrapper::EMPTY_VECTOR, error)) {
			LOG(ERROR) << " could not add child to sequential node created on replcia queue";
			return false;
		}
		return true;
	}

	void ZkClientDn::handleReplicateCmds(const char *path) {
		LOG(INFO) << "handling replicate watcher";
		std::vector <std::uint8_t> empty_data = std::vector<std::uint8_t>(0);
		std::vector<std::string> blocks_to_replicate;
		const std::string children_path(path);
		std::string work_item;
		int error;

		// TODO go through all of the work items

		// take something off the queue
		if (!zkqueue::peek(zk, children_path, work_item, error)) {
			LOG(ERROR) << "could not pop work-item off queue";
		}

		// get the dn targets
		LOG(INFO) << "getting dns at " << work_item;
		std::vector<std::string> datanode_targets;
		while (datanode_targets.size() == 0) {
			if (!zk->get_children(work_item, datanode_targets, error)) {
				LOG(ERROR) << "could not get the datande targets for the block" << error;
				return;
			}
		}

		// TODO after getting block info, check if I already have block, if so, just stop

		// get the block information
		uint64_t block_id;
		std::vector<uint8_t> data(sizeof(uint64_t));
		if (!zk->get(work_item, data, error)) {
			LOG(ERROR) << "could not read block id from work item";
		}
		memcpy(&block_id, &data[0], sizeof(uint64_t));
		LOG(INFO) << "replicating " << std::to_string(block_id);

		if (!zk->get(BLOCK_LOCATIONS + std::to_string(block_id), data, error)) {
			LOG(ERROR) << "could not get the block length for " << block_id << " because of " << error;
			return;
		}
		uint64_t block_length;
		memcpy(&block_length, &data[0], sizeof(uint64_t));
		hadoop::hdfs::ExtendedBlockProto block_proto;
		buildExtendedBlockProto(&block_proto, block_id, block_length);
		LOG(INFO) << "replicating  length " << block_length << " with targets size " << datanode_targets.size();

		// now read from the target
		data.resize(sizeof(DataNodePayload));
		for (auto dn_target_id : datanode_targets) {
			LOG(INFO) << " looking at target " << dn_target_id;
			// get the connection info for the datanode
			std::vector<std::string> split_address;
			boost::split(split_address, dn_target_id, boost::is_any_of(":"));
			if (split_address.size() != 2) {
				LOG(ERROR) << "dn id should just be ip:port";
				continue;
			}
			// get the port
			std::string dn_ip = split_address[0];
			DataNodePayload dn_target_info;
			if (!zk->get(HEALTH_BACKSLASH + dn_target_id + STATS, data, error)) {
				LOG(ERROR) << "failed to read target dn payload" << error;
				continue;
			}
			memcpy(&dn_target_info, &data[0], sizeof(DataNodePayload));
			uint32_t xferPort = dn_target_info.xferPort;
			// actually do the inter datanode communication
			server->replicate(block_length, dn_ip, std::to_string(xferPort), block_proto);
		}

		// TODO delete work item from the queue

		// TODO reattach the watcher, check if there are on work-items on me, if so, do this whole shit over again

	}

	void ZkClientDn::thisDNReplicationQueueWatcher(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx){
		ZkClientDn* dn_client = static_cast<ZkClientDn*>(watcherCtx);
		dn_client->handleReplicateCmds(path);
	}

	// TODO: Get children and for each of them delete it from raw disk IO (call delete block) and and say block deleted
	// and then re-attach watcher. Before need to call get children agian and see if there are more delete commands
	// Make sure to delete actual work item from queue
	void ZkClientDn::thisDNDeleteQueueWatcher(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx){
		LOG(INFO) << "Delete watcher triggered on path: " << path;
	}

	ZkClientDn::~ZkClientDn() {
		zk->close();
	}

	std::string ZkClientDn::build_datanode_id(DataNodeId data_node_id) {
		return data_node_id.ip + ":" + std::to_string(data_node_id.ipcPort);
	}
	
	bool ZkClientDn::sendStats(uint64_t free_space, uint32_t xmits) {
		int error_code;
		
		std::string id = build_datanode_id(data_node_id);
		data_node_payload.free_bytes = free_space;
		data_node_payload.xmits = xmits;
		std::vector<uint8_t> data;
		data.resize(sizeof(DataNodePayload));
		memcpy(&data[0], &data_node_payload, sizeof(DataNodePayload));
		if (!zk->set(HEALTH_BACKSLASH + id + STATS, data, error_code)) {
			LOG(ERROR) << CLASS_NAME <<  "Failed setting /health/<data_node_id>/stats with error " << error_code;
			return false;
		}
		return true;
	}

	bool ZkClientDn::buildExtendedBlockProto(hadoop::hdfs::ExtendedBlockProto* eb, const std::uint64_t& block_id,
											 const uint64_t& block_size) {
		eb->set_poolid("0");
		eb->set_blockid(block_id);
		eb->set_generationstamp(1);
		eb->set_numbytes(block_size);
		return true;
	}
}
#endif //RDFS_ZK_CLIENT_DN_H
