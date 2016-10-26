#ifndef RDFS_ZK_CLIENT_DN_H
#define RDFS_ZK_CLIENT_DN_H

#include "zk_client_common.h"

namespace zkclient {

class ZkClientDn : public ZkClientCommon {

public:
    ZkClientDn(const std::string& id, const std::string& zkAddress);
    ~ZkClientDn();
    void registerDataNode();
    bool addBlock(const std::string& fileName, std::vector<std::string> & dataNodes) const;


private:
    const std::string id;
};

}

#endif //RDFS_ZK_CLIENT_DN_H
