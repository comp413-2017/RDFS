#ifndef RDFS_ZK_CLIENT_DN_CC
#define RDFS_ZK_CLIENT_DN_CC

#include "zk_client_dn.h"

namespace zkclient{

ZkClientDn::ZkClientDn(const std::string& ipAndHost) : ipAndHost(ipAndHost) {

}

void ZkClientDn::registerDataNode() {
    // no-op
}

}

#endif //RDFS_ZK_CLIENT_DN_H

