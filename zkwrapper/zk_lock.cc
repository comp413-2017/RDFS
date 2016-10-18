//
// Created by Nicholas Kwon on 10/15/16.
//
#include "zk_lock.h"
#include <iostream>
const std::string ZKLock::lock_path = "/_locknode_";

std::string ZKLock::generate_uuid() {
    char uuid_data[16];
    auto uuid = boost::uuids::random_generator()();
    memcpy(uuid_data, &uuid, 16);
    return std::string(uuid_data);
}


int ZKLock::lock() {
    std::mutex mtx;
    std::condition_variable cv;
    std::replace(path_to_lock.begin(), path_to_lock.end(), '/', ':');
    if (zkWrapper.exists(lock_path, 0) < 0) {
        zkWrapper.create(lock_path, "", 0, 0);
    }
    std::string my_locknode(lock_path + "/" + path_to_lock);
    if (zkWrapper.exists(my_locknode, 0) < 0) {
        zkWrapper.create(my_locknode, "", 0, 0);
    }
    std::string my_lock(my_locknode + "/lock-");
    auto uuid = generate_uuid();
    if (zkWrapper.create(my_lock, uuid, uuid.size(), ZOO_SEQUENCE | ZOO_EPHEMERAL) < 0) {
        return -1;
    }
    while (true) {
        auto children = zkWrapper.get_children(my_locknode, 0);
        std::sort(children.begin(), children.end());
        int i = 0;
        int my_index = -1;
        for (auto child : children) {
            auto path_to_child = my_locknode + "/" + child;
            auto child_uuid = zkWrapper.get(path_to_child, 0);
            if (child_uuid.compare(uuid) == 0) {
                locknode_with_seq = std::string(path_to_child);
                if (i == 0) {
                    return 0;
                }
                my_index = i;
            }
            i++;
        }
        if (my_index == -1) {
            return -1;
        }
        auto notify = [] (zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx){
            auto cvar = reinterpret_cast<std::condition_variable *>(watcherCtx);
            cvar->notify_one();
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