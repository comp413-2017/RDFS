#ifndef RDFS_ZK_CLIENT_DN_H
#define RDFS_ZK_CLIENT_DN_H

#include "zk_client_common.h"
#include <atomic>
#include <google/protobuf/message.h>
#include "hdfs.pb.h"

class TransferServer;

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
	uint64_t disk_bytes;	//total space on disk
	uint64_t free_bytes;	//free space on disk
	uint32_t xmits;			//current number of xmits
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
	ZkClientDn(const std::string& ip, const std::string& zkIpAndAddress,
		uint64_t total_disk_space, const uint32_t ipcPort = 50020, const uint32_t xferPort = 50010);

	ZkClientDn(const std::string& ip, std::shared_ptr <ZKWrapper>,
		uint64_t total_disk_space, const uint32_t ipcPort = 50020, const uint32_t xferPort = 50010);
	~ZkClientDn();

	/**
	* Registers this DataNode with Zookeeper.
	*/
	void registerDataNode(const std::string& ip, uint64_t total_disk_space, const uint32_t ipcPort, const uint32_t xferPort);

	/**
	* Informs Zookeeper when the DataNode has received a block. Adds an acknowledgment
	* and creates a node for the DN in the block's block_locations.
	* @param uuid The UUID of the block received by the DataNode.
	* @param size_bytes The number of bytes in the block
	* @return True on success, false on error.
	*/
	bool blockReceived(uint64_t uuid, uint64_t size_bytes);

	bool sendStats(uint64_t free_space, uint32_t xmits);

	void setTransferServer(std::shared_ptr<TransferServer>& server);

	/**
	* Push the blockid onto the replication queue belonging to dn_name
	* @param dn_name the queue to add onto
	* @param blockid the id of the block to be replicated
	* @return true if success
	*/
	bool push_dn_on_repq(std::string dn_name, uint64_t blockid);

	bool poll_replication_queue();

	std::string get_datanode_id();

private:

	std::shared_ptr<TransferServer> server;
	/**
	* Builds a string of the DataNode ID.
	* @param data_node_id The DataNode's DataNodeId object, containing the IP and port.
	* @return The ID string
	*/

	void processDeleteQueue(std::string path);

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
	void initWorkQueue(std::string queueName, void (*watchFuncPtr)(zhandle_t *, int, int, const char *, void *));

	/**
	 * Handle all of the work items on path
	 */
	void handleReplicateCmds(const std::string& path);

	static void thisDNReplicationQueueWatcher(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx);
	static void thisDNDeleteQueueWatcher(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx);

	bool find_datanode_with_block(const std::string &block_uuid_str, std::string &datanode, int &error_code);

	/**
	 * Copied from nn client, create block proto
	 */
	bool buildExtendedBlockProto(hadoop::hdfs::ExtendedBlockProto* eb, const std::uint64_t& block_id,
    											 const uint64_t& block_size);


};
}

#endif //RDFS_ZK_CLIENT_DN_H
