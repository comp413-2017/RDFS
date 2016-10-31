//
// Created by Prudhvi Boyapalli on 10/3/16.
// 
//
// Modified by Zhouhan Chen on 10/4/16.
//

#include <iostream>
#include <string.h>
#include <vector>
#include "zkwrapper.h"
#include <zookeeper.h>
#include <easylogging++.h>

int init = 0;
zhandle_t *zh;
clientid_t myid;

const std::vector <std::uint8_t>
ZKWrapper::EMPTY_VECTOR = std::vector<std::uint8_t>(0);

/**
 * TODO
 *
 * @param zzh zookeeper handle
 * @param type type of event
 * @param state state of the event
 * @param path path to the watcher node
 * @param watcherCtx the state of the watcher
 */
void watcher(zhandle_t *zzh,
		int type,
		int state,
		const char *path,
		void *watcherCtx) {
	LOG(INFO) << "[Global watcher] Watcher triggered on path '" << path << "'"
		;
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

std::string ZKWrapper::translate_error(int errorcode) {
	std::vector <std::uint8_t> vec(MAX_PAYLOAD);
	//std::string message = error_message[errorcode];
	std::string message;
	message = error_message.at(errorcode);
	return message;
}

ZKWrapper::ZKWrapper(std::string host, int &error_code, std::string root) : root(root) {
	// TODO: Move these default values to some readable CONSTANT value
	zh = zookeeper_init(host.c_str(), watcher, 10000, 0, 0, 0);
	if (!zh) {
		LOG(ERROR) << "zk init failed!";
		error_code = -999;
	}
	init = 1;
    if (root.size() != 0) {
        bool root_exists;
        if (!exists(root, root_exists, error_code, false)){
            LOG(ERROR) << "Failed to check if root directory " << root << " exists " << error_code;
            return;
        }
        if (!root_exists) {
            if (!recursive_create(root, EMPTY_VECTOR, error_code, false)) {
                LOG(ERROR) << "Failed to create root directory " << root << " with error " << error_code;
            }
        }
    }
}

std::string ZKWrapper::prepend_zk_root(const std::string& path) const {
    if (root.size() == 0) {
        return path;
    }
    if (path == "/") {
        return root;
    }
    return root + path;
}

std::string ZKWrapper::removeZKRoot(const std::string& path) const {
    return path.substr(root.size());
}

/* Wrapper Implementation of Zookeeper Functions */

bool ZKWrapper::create(const std::string &path,
		const std::vector <std::uint8_t> &data,
		int &error_code, bool prepend_root) const {
	if (!init) {
		LOG(ERROR) << "Attempt to create before init!";
		error_code = -999;
		return false;
	}

	int rc = zoo_create(zh,
            prepend_root ? prepend_zk_root(path).c_str() : path.c_str(),
			reinterpret_cast<const char *>(data.data()),
			data.size(),
			&ZOO_OPEN_ACL_UNSAFE,
			0,
			nullptr,
			0);
	error_code = rc;
	if (!rc)
		return true;
	return false;
}

// TODO: Modify this
bool ZKWrapper::create_sequential(const std::string &path,
		const std::vector <std::uint8_t> &data,
		std::string &new_path,
		bool ephemeral,
		int &error_code) const {

	LOG(INFO) << "Starting sequential for " << path;
	if (!init) {
		LOG(ERROR) << "Attempt to create before init!";
		return false;
	}
	int flag = ZOO_SEQUENCE;
	if (ephemeral) {
		flag = flag | ZOO_EPHEMERAL;
	}
	LOG(INFO) << "Attempting to generate new path" << new_path;
	int len = prepend_zk_root(path).size();
	new_path.resize(MAX_PATH_LEN);
	int rc = zoo_create(zh,
			prepend_zk_root(path).c_str(),
			reinterpret_cast<const char *>(data.data()),
			data.size(),
			&ZOO_OPEN_ACL_UNSAFE,
			flag,
			reinterpret_cast<char *>(&new_path[0]),
			MAX_PATH_LEN);
	error_code = rc;
	if (rc) { // Z_OK is 0, so if we receive anything else fail
        LOG(ERROR) << "Create for " << prepend_zk_root(path) << " failed " << rc;
		// TODO: Report the proper error
		return false;
	}
	int i = 0;
	LOG(INFO) << "NEW path is " << new_path;
	new_path.resize(len+10);
    new_path = removeZKRoot(new_path);
	LOG(INFO) << "NEW path is now this" << new_path;
	return true;
}

bool ZKWrapper::recursive_create(const std::string &path,
		const std::vector <std::uint8_t> &data,
		int &error_code, bool prepend_root) const {

	for (int i=1; i<path.length(); ++i){
		if (path[i] == '/'){
            LOG(INFO) << "Generating " << path.substr(0, i);
			if (!create(path.substr(0, i), ZKWrapper::EMPTY_VECTOR, error_code, prepend_root)){
				if (error_code != ZNODEEXISTS){
					return false;
				}
			}
			error_code = ZOK;
		}
	}
    LOG(INFO) << "Generating " << path;
	return create(path, data, error_code, prepend_root);

}

bool ZKWrapper::wget(const std::string &path,
		std::vector <std::uint8_t> &data,
		watcher_fn watch,
		void *watcherCtx,
		int &error_code) const {
	// TODO: Make this a constant value. Define a smarter retry policy for oversized data
	int len = 0;
	struct Stat stat;
	error_code = zoo_wget(zh,
            prepend_zk_root(path).c_str(),
			watch,
			watcherCtx,
			reinterpret_cast<char *>(data.data()),
			&len,
			&stat);
	if (error_code != ZOK) {
		return false;
	}
	data.resize(len);
	return true;
}

bool ZKWrapper::get(const std::string &path,
		std::vector <std::uint8_t> &data,
		int &error_code) const {

	// TODO: Make this a constant value. Define a smarter retry policy for oversized data
	struct Stat stat;
	int len = MAX_PAYLOAD;
	// TODO: Perhaps we can be smarter about this
	// LOG(INFO) << "Data resizing to " << len;
	data.resize(len);
	// LOG(INFO) << "Data resizing to 1;" << data.size();
	error_code = zoo_get(zh,
            prepend_zk_root(path).c_str(),
			0,
			reinterpret_cast<char *>(data.data()),
			&len,
			&stat);
	if (error_code != ZOK) {
		return false;
	}
	data.resize(len);
	return true;
}

bool ZKWrapper::set(const std::string &path,
		const std::vector <std::uint8_t> &data,
		int &error_code,
		int version) const {

	error_code = zoo_set(zh,
            prepend_zk_root(path).c_str(),
			reinterpret_cast<const char *>(data.data()),
			data.size(),
			version);
	if (error_code != ZOK) {
		return false;
	}
	return true;
}

bool ZKWrapper::exists(const std::string &path,
		bool &exist,
		int &error_code,
        bool prepend_root) const {
	// TODO: for now watch argument is set to 0, need more error checking
	int rc = zoo_exists(zh, prepend_root ? prepend_zk_root(path).c_str() : path.c_str(), 0, 0);
	error_code = rc;
	if (rc == ZOK) {
		exist = true;
		return true;
	} else if (rc == ZNONODE) {
		exist = false;
		return true;
	} else {
		// NOTE: value exist is not updated in this case
		return false;
	}
}

bool ZKWrapper::wexists(const std::string &path,
		bool &exist,
		watcher_fn watch,
		void *watcherCtx,
		int &error_code) const {
	struct Stat stat;
	int rc = zoo_wexists(zh, prepend_zk_root(path).c_str(), watch, watcherCtx, &stat);
	error_code = rc;
	if (rc == ZOK) {
		exist = true;
		return true;
	} else if (rc == ZNONODE) {
		exist = false;
		return true;
	} else {
		// NOTE: value exist is not updated in this case
		return false;
	}
}

bool ZKWrapper::delete_node(const std::string &path, int &error_code) const {
	// NOTE: use -1 for version, check will not take place.
	error_code = zoo_delete(zh, prepend_zk_root(path).c_str(), -1);
	if (error_code != ZOK) {
		return false;
	}
	return true;
}

// TODO: Modify
bool ZKWrapper::recursive_delete(const std::string &path, int &error_code) const {
	bool root = ("/" == path);
	bool directory = path[path.size() - 1] == '/';
	int rc = 0;

	std::string znodePath = directory ? path.substr(0, path.size() - 1) : path;
	std::vector <std::string> children;
	get_children(root ? "/" : znodePath, children, rc);

	for (std::string child : children) {
		std::string newPath = znodePath + "/" + child;
		int result = recursive_delete(newPath, error_code);
		rc = (result != 0) ? result : rc;
	}

	if (!directory) {
		int result = delete_node(path, error_code); // return 0 on success
		rc = (result != 0) ? result : rc;
	}

	if (error_code != ZOK) {
		return false;
	}
	return true;
}

bool ZKWrapper::get_children(const std::string &path,
		std::vector <std::string> &children,
		int &error_code) const {

	struct String_vector stvector;
	struct String_vector *vector = &stvector;
	error_code = zoo_get_children(zh, prepend_zk_root(path).c_str(), 0, vector);
	if (error_code != ZOK) {
		return false;
	}

	int i;
	for (i = 0; i < stvector.count; i++) {
		children.push_back(stvector.data[i]);
	}
    children.resize(stvector.count);
	return true;
}

bool ZKWrapper::wget_children(const std::string &path,
		std::vector <std::string> &children,
		watcher_fn watch,
		void *watcherCtx,
		int &error_code) const {

	struct String_vector stvector;
	struct String_vector *vector = &stvector;
	error_code = zoo_wget_children(zh, prepend_zk_root(path).c_str(), watch, watcherCtx, vector);
	if (error_code != ZOK) {
		return false;
	}

	int i;
	for (i = 0; i < stvector.count; i++) {
		children.push_back(stvector.data[i]);
	}
	return true;
}

/* Multi-Operations */

std::shared_ptr <ZooOp> ZKWrapper::build_create_op(const std::string &path,
		const std::vector <std::uint8_t> &data,
		const int flags) const {
	auto op = std::make_shared<ZooOp>(prepend_zk_root(path), data);
	zoo_create_op_init(op->op,
			op->path,
			op->data,
			op->num_bytes,
			&ZOO_OPEN_ACL_UNSAFE,
			flags,
			nullptr,
			0);
	return op;
}

std::shared_ptr <ZooOp> ZKWrapper::build_delete_op(const std::string &path,
		int version) const {
	auto op = std::make_shared<ZooOp>(prepend_zk_root(path), ZKWrapper::EMPTY_VECTOR);
	zoo_delete_op_init(op->op, op->path, version);
	return op;
}

std::shared_ptr <ZooOp> ZKWrapper::build_set_op(const std::string &path,
		const std::vector <std::uint8_t> &data,
		int version) const {
	auto op = std::make_shared<ZooOp>(prepend_zk_root(path), data);
	zoo_set_op_init(op->op,
			op->path,
			op->data,
			op->num_bytes,
			version,
			nullptr);
	return op;
}

int ZKWrapper::execute_multi(const std::vector <std::shared_ptr<ZooOp>> ops,
		std::vector <zoo_op_result> &results) const {
	results.resize(ops.size());
	std::vector <zoo_op_t> trueOps = std::vector<zoo_op_t>();
	for (auto op : ops) {
		trueOps.push_back(*(op->op));
	}
	return zoo_multi(zh, ops.size(), &trueOps[0], &results[0]);
}

std::vector <uint8_t> ZKWrapper::get_byte_vector(const std::string &string) {
	std::vector <uint8_t> vec(string.begin(), string.end());
	return vec;
}

void ZKWrapper::close() {
	zookeeper_close(zh);
}


