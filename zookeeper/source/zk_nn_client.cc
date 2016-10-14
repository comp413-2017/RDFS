#ifndef RDFS_ZKNNCLIENT_CC
#define RDFS_ZKNNCLIENT_CC

#include "../include/zk_nn_client.h"
#include <iostream>


namespace zkclient{

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

}

#endif
