#ifndef RDFS_ZK_CLIENT_DN_H
#define RDFS_ZK_CLIENT_DN_H

#include "zk_client_common.h"

namespace zkclient{

class ZkClientDn : public ZkClientCommon {

public:
    ZkClientDn(const std::string& ipAndHost);
    void registerDataNode();

private:
    std::string ipAndHost;

};

}

#endif //RDFS_ZK_CLIENT_DN_H
