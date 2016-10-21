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

const std::vector<std::uint8_t> ZKWrapper::EMPTY_VECTOR = std::vector<std::uint8_t>(0);

/**
 * TODO
 * @param zzh zookeeper handle
 * @param type type of event
 * @param state state of the event
 * @param path path to the watcher node
 * @param watcherCtx the state of the watcher
 */
void watcher(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx) {
    std::cout << "[Global watcher] Watcher triggered on path '" << path << "'" << std::endl;
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
    }
}

/*
 * tranlsate numerical error code to zookeeper error string
 * @param errorcode an integer 
 * @return a string translation of the error code
 */
std::string ZKWrapper::translate_error(int errorcode) {
	std::vector<std::uint8_t> vec(MAX_PAYLOAD);
	//std::string message = error_message[errorcode];
	std::string message;
	message =  error_message.at(errorcode);
	return message;
}

/**
 * Initializes zookeeper
 * @param host The location of where Zookeeper is running. For local development
 * @param errorcode pointer of int, if set to 0, there is no error; otherwise, an error code is returned. The meaning of an error code can be retrieved from translate_error() (to be implemented) 
 *             this will usually be 'localhost:2181'
 */
ZKWrapper::ZKWrapper(std::string host, int* errorcode) {
    zh = zookeeper_init(host.c_str(), watcher, 10000, 0, 0, 0);
	if (!zh) {
        fprintf(stderr, "zk init failed!");
        *errorcode = -1;
    }
    init = 1;
}


/* Wrapper Implementation of Zookeeper Functions */

/**
 * Create a znode in zookeeper
 * @param path The location of the new znode within the zookeeper structure
 * @param data The data contained in this znode
 * @param flag The zookeeper create flags: ZOO_EPHEMERAL and/or ZOO_SEQUENCE
 * @return 0 if the operation is successful, non-zero error code otherwise
 */
bool ZKWrapper::create(const std::string &path, const std::vector<std::uint8_t> &data, int* errorcode, int flag) const {
    if (!init) {
        fprintf(stderr, "Attempt to create before init!");
        exit(1); // Error handle
    }
    int rc = zoo_create(zh, path.c_str(), reinterpret_cast<const char *>(data.data()), data.size(), &ZOO_OPEN_ACL_UNSAFE, flag,
                        nullptr, 0);
	*errorcode = rc;
    if (!rc)
		return true;
	return false;
}

/**
 * Creates a sequential znode
 * @param path The path to the new sequential znode, must end in "-" like: /foo/bar-
 * @param data The data contained in this znode
 * @param new_path Will contain the value of the newly created path
 * @param ephemeral If true, the created node will ephemeral
 * @return 0 if the operation is successful, non-zero error code otherwise
 */
bool ZKWrapper::create_sequential(const std::string &path, const std::vector<std::uint8_t> &data, std::string &new_path,
                                 bool ephemeral) const {
    if (!init) {
        fprintf(stderr, "Attempt to create before init!");
        exit(1); // Error handle
    }
    int flag = ZOO_SEQUENCE;
    if (ephemeral){
        flag = flag | ZOO_EPHEMERAL;
    }
    new_path.resize(MAX_PATH_LEN);
    int rc = zoo_create(zh, path.c_str(), reinterpret_cast<const char *>(data.data()), data.size(), &ZOO_OPEN_ACL_UNSAFE, flag,
                        reinterpret_cast<char *>(&new_path[0]), MAX_PATH_LEN);
    if (rc != ZOK) {
        fprintf(stderr, "error %d in zoo_create\n", rc);
        if (rc == ZNODEEXISTS) {
            fprintf(stderr, "Node %s already exists.\n",
                    path.c_str()); // TODO: add more error code checking
            exit(1); // TODO: Handle error
        }
    }
    int i = 0;
    while (new_path[i] != '\0'){
        i++;
    }
    new_path.resize(i);
	if (!rc)
		return true;
	return false;
}

  /**
   * Recursively creates a new znode, non-existent path components will be created
   * @param path The path to create,
   * @param data The data to store in the new ZNode
   * @return 0 if the operation is successful, non-zero error code otherwise
   */
int ZKWrapper::recursive_create(const std::string &path, const std::vector<std::uint8_t> &data) const {
    if (!exists(path, 0)) { // If the path exists (0), then do nothing
        // TODO: Should we overwrite existing data?
        return 0;
    } else { // Else recursively generate the path
        // TODO: We can use a multi-op at this point
        size_t index = path.find_last_of("/");
        std::vector<std::uint8_t> vec;
        if (recursive_create(path.substr(0, index), vec)) {
            return 1;
        }
        // std::cout << "Recursively creating " << path << std::endl;
        return create(path, data, 0);
    }
}

/**
 * This function is similar to 'get' except it allows one to specify
 * a watcher object rather than a boolean watch flag.
 *
 * @param path The path to the node
 * @param watch A watcher function
 * @param watcherCtx User specific data, will be passed to the watcher callback.
 * @return Znode value as a stream of bytes
 */
std::vector<std::uint8_t> ZKWrapper::wget(const std::string &path, watcher_fn watch, void* watcherCtx) const {
    // TODO: Make this a constant value. Define a smarter retry policy for oversized data
    std::vector<std::uint8_t> vec(MAX_PAYLOAD);
    int len = 0;
    struct Stat stat;
    int rc = zoo_wget(zh, path.c_str(), watch, watcherCtx, reinterpret_cast<char *>(vec.data()), &len, &stat);
    if (rc != ZOK) {
        printf("Error when getting!");
    }
    vec.resize(len);
    return vec;
}

/**
 * Gets the data associated with a node
 * @param path The path to the node
 * @param watch If nonzero, a watch will be set at the server to notify
 * @return Znode value as a stream of bytes
 */
std::vector<std::uint8_t> ZKWrapper::get(const std::string &path) const {
    // TODO: Make this a constant value. Define a smarter retry policy for oversized data
    std::vector<std::uint8_t> vec(MAX_PAYLOAD);
    struct Stat stat;
    int len = MAX_PAYLOAD;
    int rc = zoo_get(zh, path.c_str(), 0, reinterpret_cast<char *>(vec.data()), &len, &stat);
    if (rc != ZOK) {
        printf("Error when getting!");
    }
    vec.resize(len);
    return vec;
}

/**
 * Sets the data in a given Znode
 * @param path The path to the node
 * @param data The data that this node should contain
 * @param version A version number indicating changes to the data at this node
 * @return 0 if the operation completed successfully, non-zero error code otherwise
 */
int ZKWrapper::set(const std::string &path, const std::vector<std::uint8_t> &data, int version) const {

    const char * me = path.c_str();
    return zoo_set(zh, path.c_str(), reinterpret_cast<const char *>(data.data()), data.size(), version);
}

/**
 * Check if a Znode exists or not.
 * @param path The path to the node
 * @return True if a Znode exists at the given path, False otherwise
 */
bool ZKWrapper::exists(const std::string &path) const {
    // TODO: for now watch argument is set to 0, need more error checking
    int rc = zoo_exists(zh, path.c_str(), 0, 0);
    if (rc == 0) {
        return true;
    }
    return false;
}

 /**
  * This function is similar to 'exists' except it allows one to specify
  * a watcher object rather than a boolean watch flag.
  * @param path The path to the Znode that needs to be checked
  * @param watch A watcher function
  * @param watcherCtx User specific data, will be passed to the watcher callback.
  * @return True if a Znode exists at the given path, False otherwise
  */
bool ZKWrapper::wexists(const std::string &path, watcher_fn watch, void* watcherCtx) const {
    struct Stat stat;
    int rc = zoo_wexists(zh, path.c_str(), watch, watcherCtx, &stat);
    if (rc == 0) {
     return true;
    }
    return false;
}

/**
 * Deletes a Znode from zookeeper
 * @param path The path to the Znode that should be deleted
 * @return 0 if the operation completed successfully, non-zero error code otherwise
 */
int ZKWrapper::delete_node(const std::string &path) const {
    // NOTE: use -1 for version, check will not take place.
    int rc = zoo_delete(zh, path.c_str(), -1);
    return (rc);
}

/**
 * Recursively deletes the Znode specified in the path and any children of that path
 * @param path The path the Znode (and its children) which will be deleted
 * @return 0 if the operation completed successfully, non-zero error code otherwise
 */
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

/**
 * This function gets a list of children of the Znode specified by the path
 * @param path The path of parent node
 * @param watch If nonzero, a watch will be set at the server to notify
 * @return A vector of children Znode names
 */
std::vector <std::string> ZKWrapper::get_children(const std::string &path) const {

    struct String_vector stvector;
    struct String_vector *vector = &stvector;
    int rc = zoo_get_children(zh, path.c_str(), 0, vector);
    // TODO: error checking on rc

    int i;
    std::vector <std::string> children;
    for (i = 0; i < stvector.count; i++) {
        children.push_back(stvector.data[i]);
    }
    return children;
}

/**
 * Similar to 'get_children', except it allows one to specify
 * a watcher object rather than a boolean watch flag.
 * @param path The path to get children of and the node to place the watch on
 * @param watch A watcher function
 * @param watcherCtx User specific data, will be passed to the watcher callback.
 * @return A vector of children Znode names
 */
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

/* Multi-Operations */

std::shared_ptr<ZooOp> ZKWrapper::build_create_op(const std::string& path, const std::vector<std::uint8_t> &data, const int flags) const {
    auto op = std::make_shared<ZooOp>(path, data);
    zoo_create_op_init(op->op, op->path, op->data, op->num_bytes, &ZOO_OPEN_ACL_UNSAFE, flags, nullptr, 0);
    return op;
}

std::shared_ptr<ZooOp> ZKWrapper::build_delete_op(const std::string& path, int version) const {
    auto op = std::make_shared<ZooOp>(path, ZKWrapper::EMPTY_VECTOR);
    zoo_delete_op_init(op->op, op->path, version);
    return op;
}

std::shared_ptr<ZooOp> ZKWrapper::build_set_op(const std::string& path, const std::vector<std::uint8_t> &data, int version) const {
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


std::vector<uint8_t> ZKWrapper::get_byte_vector(const std::string &string){
    std::vector<uint8_t> vec(string.begin(), string.end());
    return vec;
}

void ZKWrapper::close() {
    zookeeper_close(zh);
}


