#ifndef RDFS_ZK_CLIENT_DN_CC
#define RDFS_ZK_CLIENT_DN_CC

#include "zk_client_dn.h"


namespace zkclient{

ZkClientDn::ZkClientDn(const std::string& id, const std::string& zkAddress) : id(id), ZkClientCommon(zkAddress) {

}

void ZkClientDn::registerDataNode() {
    // TODO: Consider using startup time of the DN along with the ip and port
    // TODO: Handle error
    zk->create("/health/data_node" + id, "", 0, ZOO_EPHEMERAL);
}

ZkClientDn::~ZkClientDn() {
    zk->close();
}

}

#endif //RDFS_ZK_CLIENT_DN_H

