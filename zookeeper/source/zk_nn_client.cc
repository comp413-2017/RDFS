#ifndef RDFS_ZKNNCLIENT_CC
#define RDFS_ZKNNCLIENT_CC

#include "../include/zk_nn_client.h"
#include "zkwrapper.h"
#include <iostream>
#include <sstream>
#include <ctime>

#include "hdfs.pb.h"
#include "ClientNamenodeProtocol.pb.h"
#include <google/protobuf/message.h>
#include <ConfigReader.h>

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

        	// TODO: Do we have to free the returned children?
        	std::vector <std::string> children = std::vector <std::string>();

		/* Place a watch on the health subtree */
		if (!zk->wget_children("/health", children, watcher_health, nullptr, errorcode)) {
	            // TODO: Handle error
        	}

		for (int i = 0; i < children.size(); i++) {
			std::cout << "[In register_watches] Attaching child to " << children[i] << ", " << std::endl;
			std::vector <std::string> ephem = std::vector <std::string>();
            		if(zk->wget_children("/health/" + children[i], ephem, watcher_health_child, nullptr, errorcode)) {
                		// TODO: Handle error
            		}
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
        	bool exists;
        	if (zk->exists(ZookeeperPath(path), exists, errorcode)) {
            		return exists;
        	} else {
            		// TODO: Handle error
        	}
	}


	// --------------------------- PROTOCOL CALLS ---------------------------------------

	void ZkNnClient::read_file_znode(FileZNode& znode_data, const std::string& path) {
		std::vector<std::uint8_t> data(sizeof(znode_data));
		if (!zk->get(ZookeeperPath(path), data, errorcode)) {
			// TODO handle error
		}
		std::uint8_t *buffer = &data[0];
		memcpy(&znode_data, buffer, sizeof(znode_data));
	}

	void ZkNnClient::file_znode_struct_to_vec(FileZNode* znode_data, std::vector<std::uint8_t>& data) {
		memcpy(&data[0], znode_data, sizeof(*znode_data));
	}

	void ZkNnClient::get_info(GetFileInfoRequestProto& req, GetFileInfoResponseProto& res) {
		const std::string& path = req.src();
		// special casee the root
		if (path == "/") {
			HdfsFileStatusProto* status = res.mutable_fs();
			FileZNode znode_data;
			set_mkdir_znode(&znode_data);
			set_file_info(status, path, znode_data);
			std::cout << "Got info for root" << std::endl;	
			return;
		}
		
		if (file_exists(path)) {
			std::cout << "File exists" << std::endl;
			// read the node into the file node struct
			FileZNode znode_data;
			read_file_znode(znode_data, path);		
	
			// set the file status in the get file info response res
			HdfsFileStatusProto* status = res.mutable_fs();
			set_file_info(status, path, znode_data);	
			std::cout << "Got info for file " << std::endl;
			std::flush(std::cout);
			return;
		}
		std::cout << "No file to get info for" << std::endl;
	}

	/**
	 * Create a node in zookeeper corresponding to a file 
	 */
	int ZkNnClient::create_file_znode(const std::string& path, FileZNode* znode_data) {
		if (!file_exists(path)) {	
			std::cout<< "Creating znode at " << path << std::endl; 
			{
				std::cout << znode_data->replication << std::endl;
				std::cout << znode_data->owner << std::endl;
				std::cout << "size of znode is " << sizeof(*znode_data) << std::endl;
			}
			// serialize struct to byte vector 
			std::vector<std::uint8_t> data(sizeof(*znode_data));
			file_znode_struct_to_vec(znode_data, data);	
			// crate the node in zookeeper 
			if (!zk->create(ZookeeperPath(path), data, errorcode)) {
				std::cout << "Create failed" << std::endl;
				return 0;
				// TODO : handle error
			}
			return 1; 
		}
		return 0;
	}

	int ZkNnClient::destroy(DeleteRequestProto& request, DeleteResponseProto& response) {
		const std::string& path = request.src();
		bool recursive = request.recursive();
		return 1;			
	}

	/**
	 * Create a file in zookeeper 
	 */ 
	int ZkNnClient::create_file(CreateRequestProto& request, CreateResponseProto& response) {
		std::cout << "hello" << std::endl;
		const std::string& path = request.src();
		const std::string& owner = request.clientname();
		bool create_parent = request.createparent();
		std::uint64_t blocksize = request.blocksize();
		std::uint32_t replication = request.replication();
		std::uint32_t createflag = request.createflag();

		if (file_exists(path))
			return 0;

		// If we need to create directories, do so 
		if (create_parent) {
			std::string directory_paths = ""; 
			auto split_path = split(path, '/');
			std::cout << split_path.size() << std::endl;
			for (int i = 0; i < split_path.size() - 1; i++) {
				directory_paths += split_path[i];
			}
			// try and make all the parents
			if (!mkdir_helper(directory_paths, true))
				return 0; 
		}

		// Now create the actual file which will hold blocks 	
		if (!file_exists(path)) {
			// create the znode
			FileZNode znode_data;
			std::cout << "sup";			
			znode_data.length = 0;
			znode_data.under_construction = 1; // TODO where are these enums
			znode_data.access_time = 0; // TODO what are these
			znode_data.modification_time = 0;
			strcpy(znode_data.owner, owner.c_str());
			strcpy(znode_data.group, owner.c_str());
			znode_data.replication = replication;
			znode_data.blocksize = blocksize;
			znode_data.filetype = 2;	

			// if we failed, then do not set any status 
			if (!create_file_znode(path, &znode_data))
				return 0;

			HdfsFileStatusProto* status = response.mutable_fs();
			set_file_info(status, path, znode_data);
		}
		return 1;
	}

	void ZkNnClient::complete(CompleteRequestProto& req, CompleteResponseProto& res) {
		// change the under construction bit
		const std::string& src = req.src();
		FileZNode znode_data;
		read_file_znode(znode_data, src);
		znode_data.under_construction = 0;
		std::vector<std::uint8_t> data(sizeof(znode_data));
		file_znode_struct_to_vec(&znode_data, data);
		if (!zk->set(ZookeeperPath(src), data, errorcode)) {
			// TODO : handle erro
		}
		res.set_result(true);		
	}

	// ------- make a directory

	/**
	 * Set the default information for a directory znode 
	 */ 
	void ZkNnClient::set_mkdir_znode(FileZNode* znode_data) {
		znode_data->length = 0;
		znode_data->access_time = 0;
		znode_data->modification_time = 0;
		znode_data->blocksize = 0;
		znode_data->replication = 0;
		znode_data->filetype = 1;
	}

	/**
	 * Make a directory in zookeeper
	 */ 
	void ZkNnClient::mkdir(MkdirsRequestProto& request, MkdirsResponseProto& response) {	
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
	bool ZkNnClient::mkdir_helper(const std::string& path, bool create_parent) {
		if (create_parent) {
			auto split_path = split(path, '/');
			bool not_exist = false;
			std::string unroll;
			std::string p_path = "";
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
			return create_file_znode(path, &znode_data);	
		}
		return true; 
	}

	void ZkNnClient::get_block_locations(GetBlockLocationsRequestProto& req, GetBlockLocationsResponseProto& res) {
		const std::string &src = req.src();
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

	void ZkNnClient::set_file_info(HdfsFileStatusProto* status, const std::string& path, FileZNode& znode_data) {
		HdfsFileStatusProto_FileType filetype;
		std::cout << znode_data.filetype << " HEY " << std::endl;
		// get the filetype, since we do not want to serialize an enum	
		switch(znode_data.filetype) {
			case(0):
				filetype = HdfsFileStatusProto::IS_DIR;
				break;
			case(1):
				filetype = HdfsFileStatusProto::IS_DIR;
                                break;
			case(2):
				filetype = HdfsFileStatusProto::IS_FILE;
				break;
			default:
				break;

		}
		
		FsPermissionProto* permission = status->mutable_permission();
		// Shorcut to set permission to 777.
		permission->set_perm(~0);
		// Set it to be a file with length 1, "foo" owner and group, 0
		// modification/access time, "0" path inode.
		status->set_filetype(filetype);
		status->set_path(path);
		status->set_length(znode_data.length);
		
		std::string owner(znode_data.owner);
		std::string group(znode_data.group);
		status->set_owner(owner);
		status->set_group(group);
		
		status->set_modification_time(znode_data.modification_time);
		status->set_access_time(znode_data.access_time);
		std::cout << "Set file info " << std::endl;
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
