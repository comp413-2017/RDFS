#ifndef RDFS_ZKNNCLIENT_H
#define RDFS_ZKNNCLIENT_H

#include "../include/zk_client_common.h"

#include "hdfs.pb.h"
#include "ClientNamenodeProtocol.pb.h"
#include <google/protobuf/message.h>
#include <ConfigReader.h>

namespace zkclient {


/**
 * This is the basic znode to describe a file 
 */ 
typedef struct
{       
	int replication;
	int blocksize;
	int under_construction;
	int filetype; 
	std::uint64_t length;
	std::uint64_t access_time;
	std::uint64_t modification_time;
	char owner[256];
	char group[256];
}FileZNode;

using namespace hadoop::hdfs;

/**
 * This is used by ClientNamenodeProtocolImpl to communicate the zookeeper. 
 */
class ZkNnClient : public ZkClientCommon {
	public:
		ZkNnClient(std::string zkIpAndAddress);
		void register_watches();
		
		//void watcher_health_child(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx);

		//void watcher_health(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx);
		
		/**
		 * These methods will correspond to proto calls that the client namenode protocol handles
		 */

		void get_info(GetFileInfoRequestProto& req, GetFileInfoResponseProto& res);
		int create_file(CreateRequestProto& request, CreateResponseProto& response);
		void get_block_locations(GetBlockLocationsRequestProto& req, GetBlockLocationsResponseProto& res);
		void mkdir(MkdirsRequestProto& req, MkdirsResponseProto& res);	
		int destroy(DeleteRequestProto& req, DeleteResponseProto& res);
		void complete(CompleteRequestProto& req, CompleteResponseProto& res);

		/**
		 * Information that the protocol might need to respond to individual rpc calls 
		 */ 	
		bool file_exists(const std::string& path);
	private:
		int errorcode;
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
		 * Split the string according to delimiter
		 */
		std::vector<std::string> split(const std::string &str, char delim);
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

		void read_file_znode(FileZNode& znode_data, const std::string& path);

		void file_znode_struct_to_vec(FileZNode* znode_data, std::vector<std::uint8_t> &data);
};

} // namespace

#endif //RDFS_ZKNNCLIENT_H

