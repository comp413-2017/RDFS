//
// Created by Nicholas Kwon on 10/15/16.
//
#include "zk_lock.h"
#include <iostream>
#include <string>
#include <sstream>
#include <vector>

std::vector<std::string> split(const std::string &s, char delim) {
    std::vector<std::string> elems;
    std::stringstream ss;
    ss.str(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
    return elems;
}

const std::string ZKLock::lock_path = "/_locknode_";

std::vector<std::uint8_t> ZKLock::generate_uuid() {
    std::vector<std::uint8_t> uuid_vec(16);
    auto uuid = boost::uuids::random_generator()();
    memcpy(uuid_vec.data(), &uuid, 16);
    return uuid_vec;
}


int ZKLock::lock() {
    std::mutex mtx;
    std::condition_variable cv;
    std::replace(path_to_lock.begin(), path_to_lock.end(), '/', ':');
    if (zkWrapper.exists(lock_path, 0) < 0) {
        zkWrapper.create(lock_path, ZKWrapper::EMPTY_VECTOR, 0);
    }
    std::string my_locknode(lock_path + "/" + path_to_lock);
    if (zkWrapper.exists(my_locknode, 0) < 0) {
        zkWrapper.create(my_locknode, ZKWrapper::EMPTY_VECTOR, 0);
    }
    std::string my_lock(my_locknode + "/lock-");

    auto rc = zkWrapper.create_sequential(my_lock, ZKWrapper::EMPTY_VECTOR, locknode_with_seq, true);
    if (rc != ZOK) {
        return -1;
    }
    auto splitted = split(locknode_with_seq, '/');

    while (true) {
        auto children = zkWrapper.get_children(my_locknode, 0);
        std::sort(children.begin(), children.end());
        auto it = std::find(children.begin(), children.end(), splitted[splitted.size() - 1]);
        auto eq = splitted[splitted.size() - 1].compare(children[0]);
        int index;
        if (it == children.end()) {
            std::cerr << "Failed to find " << splitted[splitted.size() - 1] << " in children!" << std::endl;
            return -1;
        }
        else {
            index = std::distance(children.begin(), it);
        }
        if (index == 0){
            return 0;
        }
        auto notify = [] (zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx){
            auto cvar = reinterpret_cast<std::condition_variable *>(watcherCtx);
            cvar->notify_all();
        };
        auto exists = zkWrapper.wexists(my_locknode + "/" + children[0], notify, &cv);
        if (exists){
            std::unique_lock<std::mutex> lck(mtx);
            cv.wait(lck);
        }
    }
}

int ZKLock::unlock(){
    if (locknode_with_seq.size() == 0){
        return -1;
    }
    return zkWrapper.delete_node(locknode_with_seq);
}