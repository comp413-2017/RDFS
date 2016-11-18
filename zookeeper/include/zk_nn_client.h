#ifndef RDFS_ZKNNCLIENT_H
#define RDFS_ZKNNCLIENT_H

#include "zk_client_common.h"

#include "hdfs.pb.h"
#include "ClientNamenodeProtocol.pb.h"
#include <google/protobuf/message.h>
#include <ConfigReader.h>
#include "util.h"

namespace zkclient {

/**
 * This is the basic znode to describe a file
 */
typedef struct
{
	uint32_t replication;
	uint64_t blocksize;
	int under_construction; // 1 for under construction, 0 for complete
	int filetype; // 0 or 1 for dir, 2 for file, 3 for symlinks (not supported)
	std::uint64_t length;
	// https://hadoop.apache.org/docs/r2.4.1/api/org/apache/hadoop/fs/FileSystem.html#setOwner(org.apache.hadoop.fs.Path, java.lang.String, java.lang.String)
	std::uint64_t access_time;
	std::uint64_t modification_time;
	char owner[256]; // the client who created the file
	char group[256];
}FileZNode;

using namespace hadoop::hdfs;

/**
 * This is used by ClientNamenodeProtocolImpl to communicate the zookeeper.
 */
class ZkNnClient : public ZkClientCommon {

	public:
		ZkNnClient(std::string zkIpAndAddress);

		/**
		 * Use this constructor to build ZkNnClient with a custom ZKWrapper. Which will allow you to set a root
		 * directory for all operations on this client
		 * @param zk_in shared pointer to a ZKWrapper
		 * @return ZkNnClient
		 */
		ZkNnClient(std::shared_ptr <ZKWrapper> zk_in);
		void register_watches();

		/**
		 * These methods will correspond to proto calls that the client namenode protocol handles
		 */

		void get_info(GetFileInfoRequestProto& req, GetFileInfoResponseProto& res);
		bool create_file(CreateRequestProto& request, CreateResponseProto& response);
		void get_block_locations(GetBlockLocationsRequestProto& req, GetBlockLocationsResponseProto& res);
		void mkdir(MkdirsRequestProto& req, MkdirsResponseProto& res);
		void destroy(DeleteRequestProto& req, DeleteResponseProto& res);
		void complete(CompleteRequestProto& req, CompleteResponseProto& res);
		void rename(RenameRequestProto& req, RenameResponseProto& res);
		/**
		 * Add block.
		 */
		bool add_block(AddBlockRequestProto& req, AddBlockResponseProto& res);

		bool previousBlockComplete(uint64_t prev_id);
		/**
		 * Information that the protocol might need to respond to individual rpc calls
		 */
		bool file_exists(const std::string& path);

		// this is public because we have not member functions in this file
		static const std::string CLASS_NAME;

		// TODO lil doc string and move to private (why does this cause compiler problems?)
		bool add_block(const std::string& fileName, u_int64_t& block_id, std::vector<std::string> & dataNodes, uint32_t replication_factor);
		bool find_datanode_for_block(std::vector<std::string>& datanodes, const std::uint64_t blockId, uint32_t replication_factor, bool newBlock = false);
		bool rename_file(std::string src, std::string dst);

		/**
		 * Look through the wait_for_acks work queue to check the replication
		 * status of the pending blocks and take an appropriate action to
		 * ensure that the blocks get replicated
		 */
		bool check_acks();
	private:

		/**
		 * Set the file status proto with information from the znode struct and the path
		 */
		void set_file_info(HdfsFileStatusProto* fs, const std::string& path, FileZNode& node);
		/**
		 * Given the filesystem path, get the full zookeeper path
		 */
		std::string ZookeeperPath(const std::string &hadoopPath);
		/**
		 * Use to read values from config
		 */
		config_reader::ConfigReader config;

		/**
		 * Crate a znode corresponding to a file of "filetype", with path "path", with
		 * znode data contained in "znode_data"
		 */
		int create_file_znode(const std::string &path, FileZNode* znode_data);

		/**
		 * Set the default information in a directory znode struct
		 */
		void set_mkdir_znode(FileZNode* znode_data);
		/**
		 * Create the directories at path. If create_parent is true, then we create
		 * all the parent directories which are not in zookeeper already. Return false
		 * if the creation did not work, true otherwise
		 */
		bool mkdir_helper(const std::string &path, bool create_parent);

		/**
		 * Read a znode corresponding to a file into znode_data
		 */
		void read_file_znode(FileZNode& znode_data, const std::string& path);

		/**
		 * Serialize a znode struct representation to a byte array to feed into zookeeper
		 */
		void file_znode_struct_to_vec(FileZNode* znode_data, std::vector<std::uint8_t> &data);

		/**
		 * Try to delete a node and log error if we couldnt and set response to false
		 */
		void delete_node_wrapper(std::string& path, DeleteResponseProto& response);

        bool destroy_helper(const std::string& path, std::vector<std::shared_ptr<ZooOp>>& ops);

		/**
		 * Creates 'num_replicas' many work items for the given 'block_uuid' in
		 * the replicate work queue, ensuring that the new replicas are not on
		 * an excluded datanode
		 */
		bool replicate_block(const std::string &block_uuid, int num_replicas, std::vector<std::string> &excluded_datanodes);

		/**
		 * Calculates the approximate number of seconds that have elapsed since
		 * the znode at the given path was created.
		 */
		int seconds_since_creation(std::string &path);

		/**
		 * Modifies the LocatedBlockProto with the proper block information
		 */
		// bool updateLocatedBlockProto(LocatedBlockProto* location, uint64_t block_id);

		/**
		 * Modifies the DatanodeInfoProto with information about the specified datanode.
		 * Datanode is represented as a string as most calls to this function follow a
		 * getChild() request. Returns true on success
		 */
		bool buildDatanodeInfoProto(DatanodeInfoProto *dn_info, const std::string &data_node);

        /**
         * Builds an empty token. Returns true on success.
         */
        bool buildTokenProto(hadoop::common::TokenProto* token);

        /**
         * Build an extended block proto. Returns true on success
         */
        bool buildExtendedBlockProto(ExtendedBlockProto* eb, const std::uint64_t& block_id,
                                            const uint64_t& block_size);

		/**
		 * Returns the current timestamp in milliseconds
		 */
		uint64_t current_time_ms();

		const int UNDER_CONSTRUCTION = 1;
		const int FILE_COMPLETE = 0;
		const int UNDER_DESTRUCTION = 2;

		const int IS_FILE = 2;
		const int IS_DIR = 1;
		// TODO: Should eventually be read from a conf file
		const int ACK_TIMEOUT = 60; // 60 second timeout when waiting for replication acknowledgements

};

} // namespace

#endif //RDFS_ZKNNCLIENT_H

