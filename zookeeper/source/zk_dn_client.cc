#ifndef RDFS_ZK_CLIENT_DN_CC
#define RDFS_ZK_CLIENT_DN_CC

#include "zk_dn_client.h"
#include "zk_lock.h"
#include <easylogging++.h>

namespace zkclient{


    const std::string ZkClientDn::CLASS_NAME = ": **ZkClientDn** : ";

	ZkClientDn::ZkClientDn(const std::string& ip, const std::string& hostname, std::shared_ptr <ZKWrapper> zk_in,
			const uint32_t ipcPort, const uint32_t xferPort) : ZkClientCommon(zk_in) {
		// TODO: refactor with the next constructor.

		data_node_id = DataNodeId();
		data_node_id.ip = ip;
		data_node_id.ipcPort = ipcPort;

		// TODO: Fill in other data_node stats, and refactor with the blow.
		data_node_payload = DataNodePayload();
		data_node_payload.ipcPort = ipcPort;
		data_node_payload.xferPort = xferPort;

		registerDataNode();
		LOG(INFO) << "Registered datanode " + build_datanode_id(data_node_id);

	}

	ZkClientDn::ZkClientDn(const std::string& ip, const std::string& hostname, const std::string& zkIpAndAddress,
            const uint32_t ipcPort, const uint32_t xferPort) : ZkClientCommon(zkIpAndAddress) {

		data_node_id = DataNodeId();
		data_node_id.ip = ip;
		data_node_id.ipcPort = ipcPort;

		// TODO: Fill in other data_node stats
		data_node_payload = DataNodePayload();
		data_node_payload.ipcPort = ipcPort;
		data_node_payload.xferPort = xferPort;

		registerDataNode();
		LOG(INFO) << "Registered datanode " + build_datanode_id(data_node_id);
	}

	bool ZkClientDn::blockReceived(uint64_t uuid) {
		int error_code;
		bool exists;

		LOG(INFO) << "DataNode received a block with UUID " << std::to_string(uuid);
        std::string id = build_datanode_id(data_node_id);

		
		bool created_correctly = true;

		ZKLock queue_lock(*zk.get(), WORK_QUEUES + WAIT_FOR_ACK_BACKSLASH + std::to_string(uuid));
		if (queue_lock.lock() != 0) {
			LOG(ERROR) << CLASS_NAME <<  "Failed locking on /work_queues/wait_for_acks/<block_uuid> " << error_code;
			created_correctly = false;
		}


		if (zk->exists(WORK_QUEUES + WAIT_FOR_ACK_BACKSLASH + std::to_string(uuid), exists, error_code)) {
			if (!exists) {
				if(!zk->create(WORK_QUEUES + WAIT_FOR_ACK_BACKSLASH + std::to_string(uuid), ZKWrapper::EMPTY_VECTOR, error_code)) {
					LOG(ERROR) << CLASS_NAME <<  "Failed to create wait_for_acks/<block_uuid> " << error_code;
					created_correctly = false;
				}
			} else {
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

		LOG(INFO) << "About to create the znode for the data ip";
		if (zk->exists(BLOCK_LOCATIONS + std::to_string(uuid), exists, error_code)) {
        	if (exists) {
        		// TODO: Add to spec the location to which we're writing the size
        		// zk->set(BLOCK_LOCATIONS + std::to_string(uuid), ZKWrapper::EMPTY_VECTOR, error_code); 
				// Add this datanode as the block's location in block_locations
				DataNodeZNode znode_data;
				strcpy(znode_data.ipPort, id.c_str());
				std::vector<std::uint8_t> data(sizeof(znode_data));
				memcpy(&data[0], &znode_data, sizeof(znode_data));
				LOG(INFO) << BLOCK_LOCATIONS + std::to_string(uuid) + "/" + id;
				if(!zk->create(BLOCK_LOCATIONS + std::to_string(uuid) + "/" + id, ZKWrappEMPTY_VECTOR, error_code, false)) {
					LOG(ERROR) << CLASS_NAME <<  "Failed creating /block_locations/<block_uuid>/<block_id> " << error_code;
					return false;
				}
        	}
			else {
				LOG(ERROR) << CLASS_NAME << "/block_locations/<block_uuid> did not exist " << error_code;
				return false;
			}
		}

		return created_correctly;
	}

	void ZkClientDn::registerDataNode() {
		// TODO: Consider using startup time of the DN along with the ip and port
		// TODO: Handle error

		int error_code;
        bool exists;

        std::string id = build_datanode_id(data_node_id);
        // TODO: Add a watcher on the health node
		if (zk->exists(HEALTH_BACKSLASH + id, exists, error_code)) {
            if (!exists) {
                if (!zk->create(HEALTH_BACKSLASH + id, ZKWrapper::EMPTY_VECTOR, error_code)) {
                    // TODO: Handle error
                }
            }
		}


        // Create an ephemeral node at /health/<datanode_id>/heartbeat
        // if it doesn't already exist. Should have a ZOPERATIONTIMEOUT
        if (zk->exists(HEALTH_BACKSLASH + id + "/heartbeat", exists, error_code)) {
            if (!exists) {
                if (!zk->create(HEALTH_BACKSLASH + id + "/heartbeat", data, error_code, true)) {
                    // TODO: Handle error
                }
            }
        }

        std::vector<uint8_t> data;
        data.resize(sizeof(DataNodePayload));
        memcpy(&data[0], &data_node_payload, sizeof(DataNodePayload));

        if (!zk->create(HEALTH_BACKSLASH + id + STATS, data, error_code)) {
            // TODO: Handle error
        }

	}


	ZkClientDn::~ZkClientDn() {
		zk->close();
	}

    std::string ZkClientDn::build_datanode_id(DataNodeId data_node_id) {
        return data_node_id.ip + ":" + std::to_string(data_node_id.ipcPort);
    }

}

#endif //RDFS_ZK_CLIENT_DN_H
