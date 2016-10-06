//
// Created by Prudhvi Boyapalli on 10/3/16.
// 
//
// Modified by Zhouhan Chen on 10/4/16.
//

#include <zookeeper.h>
#include "zkwrapper.h"
#include <string.h>

#include <iostream>


int init = 0;
zhandle_t *zh;
clientid_t myid;
/** Watcher function -- empty for this example, not something you should
 * do in real code */
void watcher(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx) {

}



ZKWrapper::ZKWrapper(std::string host) {
    zh = zookeeper_init(host.c_str(), watcher, 10000, 0, 0, 0);
    if (!zh) {
	fprintf(stderr, "zk init failed!");
        exit(1);
    }
        
    //zh = handle;
    init = 1;

}


int ZKWrapper::create(const std::string& path, const std::string& data, const int num_bytes) const {
    if (!init) {
	    fprintf(stderr, "Attempt to create before init!");
        exit(1); // Error handle
    }
    int rc = zoo_create(zh, path.c_str(), data.c_str(), num_bytes, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
    if (rc != 0) {
	    fprintf(stderr,"error %d in zoo_create\n", rc);
        if (rc = ZNODEEXISTS) {
            fprintf(stderr, "Node %s already exists.\n", path.c_str()); // TODO: add more error code checking
            exit(1); // TODO: Handle error
        }
    }
    return (rc);
}

/**
 * Recursively create a path if its parent directories do not exist
 *
 * @param path path to create
 * @param data data to store in the new ZNode
 * @param num_bytes number of bytes to store
 * @return 0 on success, 1 on failure
 */
int ZKWrapper::recursiveCreate(const std::string& path, const std::string& data, const int num_bytes) const {
    if (!exists(path)) { // If the path exists (0), then do nothing
        // TODO: Should we overwrite existing data?
        return 0;
    } else { // Else recursively generate the path
        size_t index = path.find_last_of("/");
        if (recursiveCreate(path.substr(0, index), "", 0)) {
            return 1;
        }
        // std::cout << "Recursively creating " << path << std::endl;
        return create(path, data, num_bytes);
    }
}

std::string ZKWrapper::get(const std::string& path) const {
    char* buffer = new char[512];
    int buf_len = sizeof(buffer);
    struct Stat stat;

    int rc = zoo_get(zh, path.c_str(), 0, buffer, &buf_len, &stat);

    if (rc) {
        printf("Error when getting!");
        exit(1); // TODO: error handling
    }

    return std::string(buffer);
}


/*
 * \param path The name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * \return 0 if path exists, 1 otherwise. Because ZOK = 0
 */
int ZKWrapper::exists(const std::string& path) const {
  // TODO: for now watch argument is set to 0, need more error checking
  int rc = zoo_exists(zh, path.c_str(), 0, 0);
  return (rc);
}

int ZKWrapper::delete_node(const std::string& path) const {
   // NOTE: use -1 for version, check will not take place.
   int rc = zoo_delete(zh, path.c_str(), -1);
   return (rc);
}

int ZKWrapper::get_children(const std::string& path) const {
   // TODO: not implemented
   // c binding function: int zoo_get_children(zhandle_t *zh, const char *path, int watch,
   //                        struct String_vector *strings);
   return (1);
}

void ZKWrapper::close() {
    zookeeper_close(zh);
}


