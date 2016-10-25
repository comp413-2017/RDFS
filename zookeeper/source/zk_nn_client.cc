#ifndef RDFS_ZKNNCLIENT_CC
#define RDFS_ZKNNCLIENT_CC

#include "../include/zk_nn_client.h"
#include "zkwrapper.h"
#include <iostreami>
#include <ctime>

#include "hdfs.pb.h"
#include "ClientNamenodeProtocol.pb.h"
#include <google/protobuf/message.h>

namespace zkclient{

	using namespace hadoop::hdfs;

	ZkNnClient::ZkNnClient(std::string zkIpAndAddress) : ZkClientCommon(zkIpAndAddress) {

	}

	

	/*
	 * A simple print function that will be triggered when 
	 * namenode loses a heartbeat
	 */
	void notify_delete() {
		printf("No heartbeat, no childs to retrieve\n");
	}

	/*
	 * Watcher for health child node (/health/datanode_)
	 */
	void watcher_health_child(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx) {
		std::cout << "[health child] Watcher triggered on path '" << path << "'" << std::endl;
		char health[] = "/health/datanode_";
		printf("[health child] Receive a heartbeat. A child has been added under path %s\n", path);

		struct String_vector stvector;
		struct String_vector *vector = &stvector;
		int rc = zoo_wget_children(zzh, path, watcher_health_child, nullptr, vector);
		int i = 0;
		if (vector->count == 0){
			notify_delete();
			//printf("no childs to retrieve\n");
		}
		while (i < vector->count) {
			printf("Children %s\n", vector->data[i++]);
		}
		if (vector->count) {
			deallocate_String_vector(vector);
		}
	}

	/*
	* Watcher for /health root node
	*/
	void watcher_health(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx) {

		struct String_vector stvector;
		struct String_vector *vector = &stvector;
		/* reinstall watcher */
		int rc = zoo_wget_children(zzh, path, watcher_health, nullptr, vector);
		std::cout << "[rc] health:" << rc << std::endl;
		int i;
		std::vector <std::string> children;
		for (i = 0; i < stvector.count; i++) {
			children.push_back(stvector.data[i]);
		}

		if (children.size() == 0){
			printf("no childs to retrieve\n");
		}

		for (int i = 0; i < children.size(); i++) {
			std::cout << "[In watcher_health] Attaching child to " << children[i] << std::endl;
			int rc = zoo_wget_children(zzh, ("/health/" + children[i]).c_str(), watcher_health_child, nullptr, vector);
			int k=0;
			while (k < vector->count) {
				printf("Children of %s:  %s\n", children[i].c_str(),  vector->data[k++]);
			}
		}

	}

	void ZkNnClient::register_watches() {

		/* Place a watch on the health subtree */
		std::vector <std::string> children = zk->wget_children("/health", watcher_health, nullptr);
		for (int i = 0; i < children.size(); i++) {
			std::cout << "[In register_watches] Attaching child to " << children[i] << ", " << std::endl;
			std::vector <std::string> ephem = zk->wget_children("/health/" + children[i], watcher_health_child, nullptr);
			/*
			   if (ephem.size() > 0) {
			   std::cout << "Found ephem " << ephem[0] << std::endl;
			   } else {
			   std::cout << "No ephem found for " << children[i] << std::endl;
			   }
			 */
		}
	}

	bool ZkNnClient::file_exists(const std::string& path) {
		return zk->exists(ZookeeperPath(path), 0) == 0;
	}


	// --------------------------- PROTOCOL CALLS ---------------------------------------

	void ZkNnClient::get_info(GetFileInfoRequestProto& req, GetFileInfoResponseProto& res) {
		const std::string& path = req.src();
		if (file_exists(path)) {
			// read the node into the file node struct
			int errorcode;
			FileZNode znode_data;
			std::vector<std::uint8_t> data(sizeof(znode_data));
			zk->get(ZookeeperPath(path), data, errorcode);
			std::uint8_t buffer* = &data[0];
			memcpy(&znode_data, buffer, sizeof(znode_data)); 
			
			// set the file status in the get file info response res
			HdfsFileStatusProto* status = res.mutable_fs();
			set_file_info(status, path, znode_data);	
		}
	}

	/**
	 * Create a node in zookeeper corresponding to a file 
	 */
	int ZkNnClient::create_file_znode(std::string& path, FileZNode* znode_data) {
		if (!file_exists(path)) {	
			// serialize struct to byte vector 
			std::vector<std::uint8_t> data(sizeof(znode_data));
			memcpy(&data[0], znode_data, sizeof(znode_data));
			// crate the node in zookeeper 
			int errorcode;
			zk->create(ZookeeperPath(path), data, errorcode);
			return errorcode; 
		}
	}

	/**
	 * Create a file in zookeeper 
	 */ 
	void ZkNnClient::create_file(CreateRequestProto& request, CreateResponseProto& response) {
		const std::string& path = request.src();
		const std::string& owner = request.clientname();
		bool create_parent = request.createparent();
		std::uint64_t blocksize = request.blocksize();
		std::uint32_t replication = request.replication();
	
		if (file_exits(path))
			return;

		// If we need to create directories, do so 
		if (create_parent) {
			std::string directory_paths = ""; 
			auto split_path = split(path, '/');
			for (int i = 0; i < split_path.size() - 1; i++) {
				directory_paths += split_paths[i];
			}
			mkdir_helper(directory_paths, true); 
		}

		// Now create the actual file which will hold blocks 	
		if (!file_exists(path)) {
			// create the znode
			FileZNode znode_data;
			
			znode_data.length = 0;
			znode_data.under_construction = 1; // TODO where are these enums
			znode_data.access_time = time();
			znode_data.modication_time = time();
			znode_data.owner = owner;
			znode_data.group = "foo"; 
			znode_data.replication = replication;
			znode_data.blocksize = blocksize;
			znode_data.filetype = HdfsFileStatusProto::IS_FILE;	

			// if we failed, then do not set any status 
			if (!create_file_znode(path, &znode_data))
				return;

			// set the return info 	
			HdfsFileStatusProto* status = response.mutable_fs();
			set_file_info(status, path, &znode_data);
		}
	}

	/**
	 * Set the default information for a directory znode 
	 */ 
	void ZkNnClient::set_mkdir_znode(FileZNode* znode_data) {
		znode_data->length = 0;
		znode_data->access_time = time();
		znode_data->modication_time = time();
		znode_data->owner = "foo"
		znode_data->blocksize = 0;
		znode_data->replication = 0;
		znode_data->filetype = HdfsFileStatusProto::IS_DIR;
	}

	/**
	 * Make a directory in zookeeper
	 */ 
	void ZkNnClient::mkdir(MkdirRequestProto& request, MkdirResponseProto& response) {	
		const std::string& path = request.src();
		bool create_parent = request.createparent();
		if (!mkdir_helper(path, create_parent)) {
			response.set_result(false);
		}
		response.set_result(true); 
	}

	/**
	 * Helper for creating a direcotyr znode. Iterates over the parents and crates them
	 * if necessary. 
	 */ 
	bool ZkNnClient::mkdir_helper(std::string& path, bool create_parent) {
		const std::string& path = request.src();
		bool create_parent = request.createparent();

		if (create_parent) {
			//TODO abstract this away to a helper function (both mkdir and create have createparent field)
			std::string p_path("");
			auto split_path = split(p_path, '/');
			bool not_exist = false;
			std::string unroll;
			for (int i = 0; i < split_path.size(); i++) {
				p_path += split_path[i] + "/";
				if (!file_exists(p_path)) {
					// keep track of the path where we start creating directories
					if (not_exist == false) {
						unroll = p_path; 
					}
					not_exist = true;
					FileZNode znode_data;
					set_mkdir_znode(&znode_data);	
					int error;
					if ((error = create_file_znode(path, &znode_data))) {
						// TODO unroll the created directories						
						return false; 
					}
				}
			}
		} 
		else {
			FileZNode znode_data;
			set_mkdir_znode(&znode_data);
			return create_file_znode(path, &znode_data));	
		}
		return true; 
	}

	void ZkNnClient::get_block_locations(GetBlockLocationsRequestProto& req, GetBlockLocationsResponseProto& res) {
		const std::string& src = req.src();
		google::protobuf::uint64 offset = req.offset();
		google::protobuf::uint64 length = req.offset();
		LocatedBlocksProto* blocks = res.mutable_locations();
		// TODO: get the actual data from zookeeper.
		blocks->set_filelength(1);
		blocks->set_underconstruction(false);
		blocks->set_islastblockcomplete(true);
		for (int i = 0; i < 1; i++) {
			LocatedBlockProto* block = blocks->add_blocks();
			block->set_offset(0);
			block->set_corrupt(false);
			// Construct extended block proto.
			ExtendedBlockProto* eb = block->mutable_b();
			eb->set_poolid("0");
			eb->set_blockid(0);
			eb->set_generationstamp(1);
			// Construct security token.
			hadoop::common::TokenProto* token = block->mutable_blocktoken();
			// TODO what do these mean
			token->set_identifier("open");
			token->set_password("sesame");
			token->set_kind("foo");
			token->set_service("bar");
			// Construct data node info objects.
			DatanodeInfoProto* dn_info = block->add_locs();
			DatanodeIDProto* id = dn_info->mutable_id();
			id->set_ipaddr("localhost");
			id->set_hostname("localhost");
			id->set_datanodeuuid("1234");
			// TODO: fill in from config
			id->set_xferport(50010);
			id->set_infoport(50020);
			id->set_ipcport(50030);
		}
	}


	// ---------------------------------------- HELPERS ----------------------------------------

	std::string ZkNnClient::ZookeeperPath(const std::string &hadoopPath){
		std::string zkpath = "/fileSystem";
		if (hadoopPath.at(0) != '/'){
			zkpath += "/";
		}
		zkpath += hadoopPath;
		if (zkpath.at(zkpath.length() - 1) == '/'){
			zkpath.at(zkpath.length() - 1) = '\0';
		}
		return zkpath;
	}

	void ZkNnClient::set_file_info(HdfsFileStatusProto* status, std::string& path, FileZNode& znode_data) {
		                        FsPermissionProto* permission = status->mutable_permission();
		// Shorcut to set permission to 777.
		permission->set_perm(~0);
		// Set it to be a file with length 1, "foo" owner and group, 0
		// modification/access time, "0" path inode.
		status->set_filetype(znode_data.filetype);
		status->set_path(path);
		status->set_length(znode_data.length);
		status->set_owner(znode_data.owner);
		status->set_group(znode_data.group);
		status->set_modification_time(znode_data.modification_time);
		status->set_access_time(znode_data.access_time);
	}

	/**
	 * Split the str by the delimiter and return a vector of the split words
	*/
        std::vector<std::string> ZkNnClient::split(const std::string &str, char delim) {
		std::stringstream ss;
		ss.str(str);
		std::string item;
		std::vector<std::string> elems;
		while (getline(ss, item, delim)) {
			elems.push_back(item);
		}
		return elems;
	}
}

#endif
