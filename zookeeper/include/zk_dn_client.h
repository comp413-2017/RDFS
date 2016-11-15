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

/**
* A struct used to write the number of bytes in a block (and
* maybe other block-related things in the future) to a ZNode
*/
typedef struct
{
	uint64_t block_size; // size of the block in bytes
}BlockZNode;

/**
* Class representing a Zookeeper-DataNode client.
* Allows a DataNode to update the state of ZK.
*/
class ZkClientDn : public ZkClientCommon {

public:
	/**
	* Initializes the Zookeeper-Datanode client.
	* @param ip The DataNode IP
	* @param hostname The DataNode hostname
	* @param zkIpAndAddress The IP and hostname of ZK
	* @param ipcPort TODO
	* @param xferPort TODO
	*/
	ZkClientDn(const std::string& ip, const std::string& hostname, const std::string& zkIpAndAddress,
			const uint32_t ipcPort = 50020, const uint32_t xferPort = 50010);

	ZkClientDn(const std::string& ip, const std::string& hostname, std::shared_ptr <ZKWrapper>,
			const uint32_t ipcPort = 50020, const uint32_t xferPort = 50010);
	~ZkClientDn();

	/**
	* Registers this DataNode with Zookeeper.
	*/
    void registerDataNode();

	/**
	* Informs Zookeeper when the DataNode has received a block. Adds an acknowledgment
	* and creates a node for the DN in the block's block_locations.
	* @param uuid The UUID of the block received by the DataNode.
	* @param size_bytes The number of bytes in the block
	* @return True on success, false on error.
	*/
    bool blockReceived(uint64_t uuid, uint64_t size_bytes);

    /**
    * Informs Zookeeper when the DataNode has deleted a block. 
    * @param uuid The UUID of the block deleted by the DataNode.
    * @param size_bytes The number of bytes in the block
    * @return True on success, false on error.
    */
    bool blockDeleted(uint64_t uuid, uint64_t size_bytes);


private:

	/**
	* Builds a string of the DataNode ID.
	* @param data_node_id The DataNode's DataNodeId object, containing the IP and port.
	* @return The ID string
	*/
    std::string build_datanode_id(DataNodeId data_node_id);

    DataNodeId data_node_id;
    DataNodePayload data_node_payload;

    static const std::string CLASS_NAME;

    /**
    * Sets up the work queue for this datanode in zookeeper, and sets the watcher
    * on that queue.  To be used for replication and deletion queues
    * @param queueName the name of the queue, i.e. replication or deletion
    * @param watchFuncPtr the watcher function to be used on the queue
    */
    void initWorkQueue(std::string queueName, void (*watchFuncPtr)(zhandle_t *, int, int, const char *, void *), std::string id);

    static void thisDNReplicationQueueWatcher(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx);
    static void thisDNDeleteQueueWatcher(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx);
};

}

#endif //RDFS_ZK_CLIENT_DN_H
