#ifndef RDFS_ZK_CLIENT_DN_CC
#define RDFS_ZK_CLIENT_DN_CC

#include "zk_dn_client.h"
#include "zk_lock.h"
#include "zk_queue.h"
#include "native_filesystem.h"
#include <easylogging++.h>

namespace zkclient{

	const std::string ZkClientDn::CLASS_NAME = ": **ZkClientDn** : ";

	ZkClientDn::ZkClientDn(const std::string& ip, std::shared_ptr <ZKWrapper> zk_in, std::shared_ptr <nativefs::NativeFS> fs_in,
		 uint64_t total_disk_space, const uint32_t ipcPort, const uint32_t xferPort) : ZkClientCommon(zk_in) {
		fs = fs_in;
		registerDataNode(ip, total_disk_space, ipcPort, xferPort);
	}

	ZkClientDn::ZkClientDn(const std::string& ip, const std::string& zkIpAndAddress, std::shared_ptr <nativefs::NativeFS> fs_in,
		uint64_t total_disk_space, const uint32_t ipcPort, const uint32_t xferPort) : ZkClientCommon(zkIpAndAddress) {
		fs = fs_in;
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

//		ZkClientDn::initWorkQueue(REPLICATE_QUEUES, id);
		ZkClientDn::initWorkQueue(DELETE_QUEUES, id);

		// Register the queue watchers for this dn
		std::vector <std::string> children = std::vector <std::string>();
		// if(!zk->wget_children(REPLICATE_QUEUES + id, children, ZkClientDn::thisDNReplicationQueueWatcher, this, error_code)){
		// 	LOG(INFO) << "getting children for replicate queue failed";
		// }
		if(!zk->wget_children(DELETE_QUEUES + id, children, ZkClientDn::thisDNDeleteQueueWatcher, this, error_code)){
			LOG(ERROR) << CLASS_NAME << "Registering delete queue watchers failed";
		}

        // // TODO: For debugging only
		// std::vector <std::uint8_t> replUUID (1);
		// std::string push_path;
		// replUUID[0] = 12;
        // push(zk, REPLICATE_QUEUES + id, replUUID, push_path, error_code);
	}

	void ZkClientDn::initWorkQueue(std::string queueName, std::string id){
        int error_code;
        bool exists;

        // Creqte queue for this datanode
        // TODO: Replace w/ actual queues when they're created
        if (zk->exists(queueName + id, exists, error_code)){
            if (!exists){
                LOG(INFO) << CLASS_NAME << "Initializing queue: " << queueName;
                if (!zk->create(queueName + id, ZKWrapper::EMPTY_VECTOR, error_code, false)){
                    LOG(ERROR) << CLASS_NAME << "Queue creation failed";
                }
            }
        }
	}

	void ZkClientDn::thisDNDeleteQueueWatcher(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx){
		LOG(INFO) << CLASS_NAME << "Delete watcher triggered on path: " << path;

		ZkClientDn *thisDn = static_cast<ZkClientDn *>(watcherCtx);
		thisDn->processDeleteQueue(path);
	}

	void ZkClientDn::processDeleteQueue(std::string path) {
		int error_code;
		bool exists;
		std::string id = build_datanode_id(data_node_id);
		uint64_t block_id;
		std::string peeked;
		std::string popped;
		std::vector<uint8_t> queue_vec(1); //TODO: Size?
		std::vector <std::string> children(1);

		if (zk->exists(path, exists, error_code)){
			if (!exists){
				LOG(ERROR) << CLASS_NAME << "Delete queue for DN " << id << " did not exist";
				return;
			}
		}

		if (!peek(zk, path, peeked, error_code)){
			LOG(ERROR) << CLASS_NAME << "Failed queue peek " << error_code;
		}

		while(peeked != path){
			LOG(INFO) << CLASS_NAME << "Found item in delete queue: " << peeked;
			if (!zk->get(peeked, queue_vec, error_code)) {
				LOG(ERROR) << "Failed to get data from peeked node " << error_code;
				if (!peek(zk, path, peeked, error_code)){
					LOG(ERROR) << CLASS_NAME << "Failed queue peek " << error_code;
				}
				continue;
			}
			memcpy(&block_id, &queue_vec[0], sizeof(queue_vec));
			LOG(INFO) << CLASS_NAME << "Deleting block with ID " << block_id;

			if (fs->rmBlock(block_id)){
				blockDeleted(block_id);
				pop(zk, path, queue_vec, popped, error_code);
			}
			else {
				LOG(ERROR) << CLASS_NAME << "Failed to delete block";
			}

			if (!peek(zk, path, peeked, error_code)){
				LOG(ERROR) << CLASS_NAME << "Failed queue peek " << error_code;
			}
		}
		if(!zk->wget_children(DELETE_QUEUES + id, children, ZkClientDn::thisDNDeleteQueueWatcher, this, error_code)){
			LOG(ERROR) << CLASS_NAME << "Registering delete queue watchers failed";
		}

		if (children.size() > 0){
			LOG(INFO) << CLASS_NAME << "Blocks were scheduled for deletion before watcher was re-attached";
		}
		else {
			LOG(INFO) << CLASS_NAME << "Delete queue processed";
		}

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
}
#endif //RDFS_ZK_CLIENT_DN_H
