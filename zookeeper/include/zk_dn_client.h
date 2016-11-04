#ifndef RDFS_ZK_CLIENT_DN_H
#define RDFS_ZK_CLIENT_DN_H

#include "zk_client_common.h"

namespace zkclient {

typedef struct
{
    std::string ip;
    uint32_t ipcPort;
} DataNodeId;


// TODO: Store hostname in payload as a vararg?
typedef struct
{
    uint32_t ipcPort;
    uint32_t xferPort;
    uint64_t disk_bytes;
    uint64_t mem_bytes;
} DataNodePayload;

typedef struct
{
    char ipPort[256];
} DataNodeZNode;

class ZkClientDn : public ZkClientCommon {

public:
    ZkClientDn(const std::string& ip, const std::string& hostname, const std::string& zkIpAndAddress,
                       const uint32_t ipcPort = 50020, const uint32_t xferPort = 50010);
    ~ZkClientDn();
    void registerDataNode();
    bool blockReceived(uint64_t uuid);


private:

    std::string build_datanode_id(DataNodeId data_node_id);

    DataNodeId data_node_id;
    DataNodePayload data_node_payload;

    static const std::string CLASS_NAME;
};

}

#endif //RDFS_ZK_CLIENT_DN_H
