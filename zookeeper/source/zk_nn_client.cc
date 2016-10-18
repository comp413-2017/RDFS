#ifndef RDFS_ZKNNCLIENT_CC
#define RDFS_ZKNNCLIENT_CC

#include "../include/zk_nn_client.h"
#include "zkwrapper.h"
#include <iostream>

#include "hdfs.pb.h"
#include "ClientNamenodeProtocol.pb.h"
#include <google/protobuf/message.h>

namespace zkclient{

	using namespace hadoop::hdfs;

	ZkNnClient::ZkNnClient(std::string zkIpAndAddress) : ZkClientCommon(zkIpAndAddress) {

	}

	void watcher(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx) {
		std::cout << "Watcher triggered on path '" << path << "'" << std::endl;
		char health[] = "/health/datanode_";
		printf("a child has been added under path %s\n", path);

		struct String_vector stvector;
		struct String_vector *vector = &stvector;
		int rc = zoo_get_children(zzh, path, 1, vector);
		int i = 0;
		if (vector->count == 0){
			printf("no childs to retrieve\n");
		}
		while (i < vector->count) {
			printf("Children %s\n", vector->data[i++]);
		}
		if (vector->count) {
			deallocate_String_vector(vector);
		}
	}

	void ZkNnClient::register_watches() {

		// Place a watch on the health subtree
		std::vector <std::string> children = zk->get_children("/health", 1); // TODO: use a constant for the path
		for (int i = 0; i < children.size(); i++) {
			std::cout << "Attaching child to " << children[i] << ", " << std::endl;
			std::vector <std::string> ephem = zk->wget_children("/health/" + children[i], watcher, nullptr);
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

	void ZkNnClient::get_info(GetFileInfoRequestProto& req, GetFileInfoResponseProto& res) {
		const std::string& path = req.src();
		if (file_exists(path)) {
			// TODO: use real data.
			HdfsFileStatusProto* status = res.mutable_fs();
			FsPermissionProto* permission = status->mutable_permission();
			// Shorcut to set permission to 777.
			permission->set_perm(~0);
			// Set it to be a file with length 1, "foo" owner and group, 0
			// modification/access time, "0" path inode.
			status->set_filetype(HdfsFileStatusProto::IS_FILE);
			status->set_path(path);
			status->set_length(1);
			status->set_owner("foo");
			status->set_group("foo");
			status->set_modification_time(0);
			status->set_access_time(0);
			// Other fields are optional, skip for now.
		}
	}

	void ZkNnClient::create_file(CreateRequestProto& request, CreateResponseProto& response) {
		const std::string& path = request.src();
		if (!file_exists(path)) {
			zk->create(ZookeeperPath(path), "foo", 0);
			HdfsFileStatusProto* status = response.mutable_fs();
			FsPermissionProto* permission = status->mutable_permission();
			// Shorcut to set permission to 777.
			permission->set_perm(~0);
			// Set it to be a file with length 1, "foo" owner and group, 0
			// modification/access time, "0" path inode.
			status->set_filetype(HdfsFileStatusProto::IS_FILE);
			status->set_path(path);
			status->set_length(1);
			status->set_owner("foo");
			status->set_group("foo");
			status->set_modification_time(0);
			status->set_access_time(0);
			// Other fields are optional, skip for now.
		}
	}


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

}

#endif
