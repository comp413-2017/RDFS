#ifndef RDFS_ZK_CLIENT_DN_CC
#define RDFS_ZK_CLIENT_DN_CC

#include "zk_client_dn.h"


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

	ZkClientDn::~ZkClientDn() {
		zk->close();
	}

}

#endif //RDFS_ZK_CLIENT_DN_H

