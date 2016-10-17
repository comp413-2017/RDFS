//
// Created by Prudhvi Boyapalli on 10/3/16.
// 
//
// Modified by Zhouhan Chen on 10/4/16.
//

#include <iostream>
#include <string.h>
#include <vector>
#include <zookeeper.h>
#include "zkwrapper.h"


int init = 0;
zhandle_t *zh;
clientid_t myid;

/** Watcher function -- empty for this example, not something you should
 * do in real code */
void watcher(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx) {
    std::cout << "Watcher triggered on path '" << path << "'" << std::endl;
    char health[] = "/health/datanode_";
    if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
            return;
        } else if (state == ZOO_AUTH_FAILED_STATE) {
            zookeeper_close(zzh);
            exit(1);
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
            zookeeper_close(zzh);
            exit(1);
        }
    } else if (type == ZOO_CREATED_EVENT) {            
        printf("Node created at %s\n", path);
        int rc = zoo_exists(zzh, path, 1, 0);                         
        if (ZOK != rc){                                                     
            printf("Problems  %d\n", rc);                                   
        }                                                                   
    } else if (type == ZOO_DELETED_EVENT) {                                 
        printf("Node deleted at %s\n", path);
        int rc = zoo_exists(zzh, path, 1, 0);                              
        if (ZOK != rc){                                                      
            printf("Problems  %d\n", rc);                                    
        }                                                                    
    } else{
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


int ZKWrapper::create(const std::string &path, const std::string &data,
                      const int num_bytes, int flag) const {
    if (!init) {
        fprintf(stderr, "Attempt to create before init!");
        exit(1); // Error handle
    }
    int rc = zoo_create(zh, path.c_str(), data.c_str(), num_bytes, &ZOO_OPEN_ACL_UNSAFE, flag, NULL, 0);
    if (rc != 0) {
        fprintf(stderr, "error %d in zoo_create\n", rc);
        if (rc == ZNODEEXISTS) {
            fprintf(stderr, "Node %s already exists.\n",
                    path.c_str()); // TODO: add more error code checking
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
int ZKWrapper::recursiveCreate(const std::string &path, const std::string &data, const int num_bytes) const {
    if (!exists(path, 0)) { // If the path exists (0), then do nothing
        // TODO: Should we overwrite existing data?
        return 0;
    } else { // Else recursively generate the path
        // TODO: We can use a multi-op at this point
        size_t index = path.find_last_of("/");
        if (recursiveCreate(path.substr(0, index), NULL, -1)) {
            return 1;
        }
        // std::cout << "Recursively creating " << path << std::endl;
        return create(path, data, num_bytes);
    }
}

/* This function is similar to zoo_get except it allows one specify
 * a watcher object rather than a boolean watch flag.
 *
 * @param path the name of the node
 * @param watch a watcher function
 * @param watcherCtx user specific data, will be passed to the watcher callback.
 * @return znode value as a stream of bytes
 */
std::string ZKWrapper::wget(const std::string &path, watcher_fn watch, void* watcherCtx) const {
    // TODO: Make this a constant value. Define a smarter retry policy for oversized data
    char *buffer = new char[512];
    int buf_len = 512;
    struct Stat stat;

    int rc = zoo_wget(zh, path.c_str(), watch, watcherCtx, buffer, &buf_len, &stat);

    if (rc) {
        printf("Error when getting!");
        exit(1); // TODO: error handling
    }

    return std::string(buffer);
}

/*
 * gets the data associated with a node
 * @param path the name of the node
 * @param watch, if nonzero, a watch will be set at the server to notify
 * @return znode value as a stream of bytes
 */
std::string ZKWrapper::get(const std::string &path, const int watch) const {
    // TODO: Make this a constant value. Define a smarter retry policy for oversized data
    char *buffer = new char[512];
    int buf_len = 512;
    struct Stat stat;

    int rc = zoo_get(zh, path.c_str(), watch, buffer, &buf_len, &stat);

    if (rc) {
        printf("Error when getting!");
        exit(1); // TODO: error handling
    }

    return std::string(buffer);
}

/**
 * check if a znode exists or not.
 * @param path The name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 *
 * @return 0 if path exists, 1 otherwise. Because ZOK = 0
 */
int ZKWrapper::exists(const std::string &path, const int watch) const {
    // TODO: for now watch argument is set to 0, need more error checking
    int rc = zoo_exists(zh, path.c_str(), watch, 0);
    return (rc);
}


int ZKWrapper::set(const std::string &path, const std::string &data, int version) const {

    const char * me = path.c_str();
    return zoo_set(zh, path.c_str(), data.c_str(), 17, version);
}

/* This function is similar to zoo_exist except it allows one specify
 * a watcher object rather than a boolean watch flag.
 */
int ZKWrapper::wexists(const std::string &path, watcher_fn watch, void* watcherCtx) const {
    struct Stat stat;
    int rc = zoo_wexists(zh, path.c_str(), watch, watcherCtx, &stat);
    return (rc);
}


int ZKWrapper::delete_node(const std::string &path) const {
    // NOTE: use -1 for version, check will not take place.
    int rc = zoo_delete(zh, path.c_str(), -1);
    return (rc);
}

int ZKWrapper::recursive_delete(const std::string path) const {
    bool root = ("/" == path);
    bool directory =  path[path.size() - 1] == '/';
    std::string znodePath = directory ? path.substr(0, path.size() - 1) : path;
    std::vector<std::string> children = get_children(root ? "/" : znodePath, 0);
    int rc = 0;
    for (std::string child : children) {
        std::string newPath = znodePath + "/" + child;
        int result = recursive_delete(newPath);
        rc = (result != 0) ? result : rc;
    }
    if (!directory) {
        int result = delete_node(path); // return 0 on success
        rc = (result != 0) ? result : rc;
    }
    return rc;
}

/* This function gets a list of children path
 * @param path path of parent node
 * @param watch, if nonzero, a watch will be set at the server to notify
 * @return a list of path
 */
std::vector <std::string> ZKWrapper::get_children(const std::string &path, const int watch) const {

    struct String_vector stvector;
    struct String_vector *vector = &stvector;
    int rc = zoo_get_children(zh, path.c_str(), watch, vector);
    // TODO: error checking on rc

    int i;
    std::vector <std::string> children;
    for (i = 0; i < stvector.count; i++) {
        children.push_back(stvector.data[i]);
    }
    return children;
}

std::vector <std::string> ZKWrapper::wget_children(const std::string &path, watcher_fn watch, void* watcherCtx) const {

    struct String_vector stvector;
    struct String_vector *vector = &stvector;
    int rc = zoo_wget_children(zh, path.c_str(), watch, watcherCtx, vector);

    int i;
    std::vector <std::string> children;
    for (i = 0; i < stvector.count; i++) {
        children.push_back(stvector.data[i]);
    }
    return children;
}


std::shared_ptr<ZooOp> ZKWrapper::build_create_op(const std::string& path, const std::string& data, const int flags) const {
    auto op = std::make_shared<ZooOp>(path, data);
    zoo_create_op_init(op->op, op->path, op->data, op->num_bytes, &ZOO_OPEN_ACL_UNSAFE, flags, nullptr, 0);
    return op;
}

std::shared_ptr<ZooOp> ZKWrapper::build_delete_op(const std::string& path, int version) const {
    auto op = std::make_shared<ZooOp>(path);
    zoo_delete_op_init(op->op, op->path, version);
    return op;
}

std::shared_ptr<ZooOp> ZKWrapper::build_set_op(const std::string& path, const std::string& data, int version) const {
    auto op = std::make_shared<ZooOp>(path, data);
    zoo_set_op_init(op->op, op->path, op->data, op->num_bytes, version, nullptr);
    return op;
}


int ZKWrapper::execute_multi(const std::vector<std::shared_ptr<ZooOp>> ops, std::vector<zoo_op_result>& results) const {
    results.resize(ops.size());
    std::vector<zoo_op_t> trueOps = std::vector<zoo_op_t>();
    for (auto op : ops) {
        trueOps.push_back(*(op->op));
    }
    return zoo_multi(zh, ops.size(), &trueOps[0], &results[0]);
}



void ZKWrapper::close() {
    zookeeper_close(zh);
}


