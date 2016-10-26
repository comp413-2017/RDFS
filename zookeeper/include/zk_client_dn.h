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

    bool generateBlockUUID(std::vector<uint8_t>& uuid) const;
    bool findDataNodeForBlock(const std::vector<uint8_t> uuid_vec, bool newBlock = false) const;


    const std::string id;
};

}

#endif //RDFS_ZK_CLIENT_DN_H
