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
				if(!zk->create(BLOCK_LOCATIONS + std::to_string(uuid) + "/" + id, ZKWrapper::EMPTY_VECTOR, error_code, true)) {
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
		ZkClientDn::initWorkQueue(REPLICATE_QUEUES, ZkClientDn::thisDNReplicationQueueWatcher);
		ZkClientDn::initWorkQueue(DELETE_QUEUES, ZkClientDn::thisDNDeleteQueueWatcher);
		LOG(INFO) << "Registered datanode " + build_datanode_id(data_node_id);
	}

	void ZkClientDn::initWorkQueue(std::string queueName, void (* watchFuncPtr)(zhandle_t *, int, int, const char *, void *)){
		int error_code;
		bool exists;
		std::string id = get_datanode_id();

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
		std::vector <std::string> children;

		if(!zk->wget_children(queueName + id, children, watchFuncPtr, this, error_code)){
			LOG(INFO) << "getting children failed";
		}

		// While we still have work items continue to pull them off
		while (children.size() > 0) {
			handleReplicateCmds((queueName + id).c_str());
			children = std::vector <std::string>();
			if(!zk->wget_children(queueName + id, children, watchFuncPtr, this, error_code)){
				LOG(INFO) << "getting children failed";
			}
		}
	}

	bool ZkClientDn::push_dn_on_repq(std::string dn_name, uint64_t blockid) {
		LOG(INFO) << "adding datanode and block to replication queue " << std::to_string(blockid);
		auto queue_path = util::concat_path(ZkClientCommon::REPLICATE_QUEUES, dn_name);
		auto my_id = build_datanode_id(data_node_id);
		std::vector<uint8_t> block_vec (sizeof(uint64_t));
		memcpy(&block_vec[0], &blockid, sizeof(uint64_t));
		int error;
        std::vector<std::shared_ptr<ZooOp>> ops;
        std::vector<zoo_op_result> results;
        auto work_item = util::concat_path(queue_path, std::to_string(blockid));
        ops.push_back(zk->build_create_op(work_item, ZKWrapper::EMPTY_VECTOR, 0));
        auto read_from = util::concat_path(work_item, my_id);
        ops.push_back(zk->build_create_op(read_from, ZKWrapper::EMPTY_VECTOR, 0));
        if (!zk->execute_multi(ops, results, error)){
            LOG(ERROR) << "Failed to create replication commands for pipelining!";
            return false;
        }
        return true;
	}

	void ZkClientDn::setTransferServer(std::shared_ptr<TransferServer>& server){
		this->server = server;
	}

	void ZkClientDn::handleReplicateCmds(const std::string& path) {
		int err;
		LOG(ERROR) << "handling replicate watcher for " << path;
		std::vector<std::string> work_items;

		if (!zk->get_children(path, work_items, err)){
			LOG(ERROR) << "Failed to get work items!";
			return;
		}
		std::vector<std::shared_ptr<ZooOp>> ops;
		for (auto &block : work_items){
			std::vector<std::string> read_from;
			auto full_work_item_path = util::concat_path(path, block);
			if (!zk->get_children(full_work_item_path, read_from, err)){
				LOG(ERROR) << "Failed to get datanode to read from!";
				return;
			}
			assert(read_from.size() == 1);

			// get block size
			std::vector<std::uint8_t> block_size_vec(sizeof(std::uint64_t));
			std::uint64_t block_size;
			if (!zk->get(util::concat_path(BLOCK_LOCATIONS, block), block_size_vec, err, sizeof(std::uint64_t))) {
				LOG(ERROR) << "could not get the block length for " << block << " because of " << err;
				return;
			}
			memcpy(&block_size, &block_size_vec[0], sizeof(uint64_t));

			//build block proto
			hadoop::hdfs::ExtendedBlockProto block_proto;
			std::uint64_t block_id;
			std::stringstream strm(block);
			strm >> block_id;
			LOG(INFO) << "Block id is " << std::to_string(block_id) << " " << block;
			buildExtendedBlockProto(&block_proto, block_id, block_size);


			std::vector<std::string> split_address;
			boost::split(split_address, read_from[0], boost::is_any_of(":"));
			assert(split_address.size() == 2);
			// get the port
			std::string dn_ip = split_address[0];
			DataNodePayload dn_target_info;
			std::vector<std::uint8_t> dn_data(sizeof(DataNodePayload));
			if (!zk->get(HEALTH_BACKSLASH + read_from[0] + STATS, dn_data, err, sizeof(DataNodePayload))) {
				LOG(ERROR) << "failed to read target dn payload" << err;
				return;
			}
			memcpy(&dn_target_info, &dn_data[0], sizeof(DataNodePayload));
			std::uint32_t xferPort = dn_target_info.xferPort;
			// actually do the inter datanode communication
			if (server->replicate(block_size, dn_ip, std::to_string(xferPort), block_proto)) {
				LOG(INFO) << "Replication successful.";
                auto read_from_path = util::concat_path(full_work_item_path, read_from[0]);
				ops.push_back(zk->build_delete_op(read_from_path));
				ops.push_back(zk->build_delete_op(full_work_item_path));
			} else {
				LOG(ERROR) << "Replication unsuccessful.";
			}
		}
		//delete work items
		std::vector<zoo_op_result> results;
		if (!zk->execute_multi(ops, results, err)){
			LOG(ERROR) << "Failed to delete sucessfully completed replicate commands!";
		}
	}

	void ZkClientDn::thisDNReplicationQueueWatcher(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx){
		ZkClientDn* dn_client = static_cast<ZkClientDn*>(watcherCtx);
		dn_client->handleReplicateCmds(dn_client->zk->removeZKRoot(path));
		// We have to reattach the watcher function
		dn_client->initWorkQueue(REPLICATE_QUEUES, ZkClientDn::thisDNReplicationQueueWatcher);
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
