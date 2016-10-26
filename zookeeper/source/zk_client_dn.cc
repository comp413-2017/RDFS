#ifndef RDFS_ZK_CLIENT_DN_CC
#define RDFS_ZK_CLIENT_DN_CC

#include "zk_client_dn.h"
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

namespace zkclient{

	ZkClientDn::ZkClientDn(const std::string& id, const std::string& zkAddress) : id(id), ZkClientCommon(zkAddress) {

	}

	void ZkClientDn::registerDataNode() {
		// TODO: Consider using startup time of the DN along with the ip and port
		// TODO: Handle error

		int error_code;
        bool exists;

        // TODO: Add a watcher on the health node
		if (zk->exists("/health/datanode_" + id, exists, error_code)) {
            if (!exists) {
                if (!zk->create("/health/datanode_" + id, ZKWrapper::EMPTY_VECTOR, error_code)) {
                    // TODO: Handle error
                }
            }
		}
        // TODO: Make ephemeral
		if (!zk->create("/health/datanode_" + id + "/health", ZKWrapper::EMPTY_VECTOR, error_code)) {
            // TODO: Handle error
        }
	}

    bool ZkClientDn::addBlock(const std::string& fileName, std::vector<std::string> & dataNodes) const {

        // TODO: Check if file is still under construction
        // TODO: Check the replication factor
        // TODO: Find datanodes
        // TODO: Generate UUID and create sequential node
        // TODO: Create ack node

        return true;
    }

    bool ZkClientDn::generateBlockUUID(std::vector<uint8_t>& uuid_vec) const {
        uuid_vec.resize(16);
        auto uuid = boost::uuids::random_generator()();
        memcpy(uuid_vec.data(), &uuid, 16);
        return true;
    }

    bool ZkClientDn::findDataNodeForBlock(const std::vector<uint8_t> uuid, bool newBlock) const {
        // TODO: Perform a search for datanodes, possibly cached
        return true;
    }


	ZkClientDn::~ZkClientDn() {
		zk->close();
	}

}

#endif //RDFS_ZK_CLIENT_DN_H

