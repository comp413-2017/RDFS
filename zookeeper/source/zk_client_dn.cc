#ifndef RDFS_ZK_CLIENT_DN_CC
#define RDFS_ZK_CLIENT_DN_CC

#include "zk_client_dn.h"
#include <easylogging++.h>

namespace zkclient{

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
	}

	bool ZkClientDn::blockReceived(uint64_t uuid) {
		
		int error_code;
		bool exists;

        std::string id = build_datanode_id(data_node_id);

		// Create block
        if (zk->exists("work_queues/wait_for_acks/" + std::to_string(uuid), exists, error_code)) {
        	if (exists) {
        		if(zk->create_ephemeral("work_queues/wait_for_acks/" + std::to_string(uuid) + "/" + id, ZKWrapper::EMPTY_VECTOR, error_code)) {
            		return true;
        		}
        	}
        	// TODO: Display error message
		}

		// TODO: Display error message
		return false;
	}

	void ZkClientDn::registerDataNode() {
		// TODO: Consider using startup time of the DN along with the ip and port
		// TODO: Handle error

		int error_code;
        bool exists;

        std::string id = build_datanode_id(data_node_id);
        // TODO: Add a watcher on the health node
		if (zk->exists("/health/" + id, exists, error_code)) {
            if (!exists) {
                if (!zk->create("/health/" + id, ZKWrapper::EMPTY_VECTOR, error_code)) {
                    // TODO: Handle error
                }
            }
		}
        // TODO: Make ephemeral
		if (!zk->create("/health/" + id + "/health", ZKWrapper::EMPTY_VECTOR, error_code)) {
            // TODO: Handle error
        }

        std::vector<uint8_t> data;
        data.resize(sizeof(DataNodePayload));
        memcpy(&data[0], &data_node_payload, sizeof(DataNodePayload));

        if (!zk->create("/health/" + id + "/stats", data, error_code)) {
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

