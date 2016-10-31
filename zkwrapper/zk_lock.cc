//
// Created by Nicholas Kwon on 10/15/16.
//
#include "zk_lock.h"

const std::string ZKLock::CLASS_NAME = ": **ZkQueue** : ";

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
    std::unique_lock<std::mutex> lck(mtx);

    std::replace(path_to_lock.begin(), path_to_lock.end(), '/', ':');

    bool exists;
    int error_code;


    std::string my_locknode(lock_path + "/" + path_to_lock);
    if (!zkWrapper.exists(my_locknode, exists, error_code)){
        LOG(ERROR) << CLASS_NAME <<  "ZKLock::lock(): Failed to check existence for " << my_locknode;
        return error_code;
    }
    if (!exists && !zkWrapper.recursive_create(my_locknode, ZKWrapper::EMPTY_VECTOR, error_code)){
        LOG(ERROR) << CLASS_NAME <<  "ZKLock::lock(): Failed to recursively create " << my_locknode;
        return error_code;
    }
    std::string my_lock(my_locknode + "/lock-");
    if (!zkWrapper.create_sequential(my_lock, ZKWrapper::EMPTY_VECTOR, locknode_with_seq, true, error_code)) {
        LOG(ERROR) << CLASS_NAME <<  "ZKLock::lock(): Failed to create sequential " << my_lock;
        return error_code;
    }

    auto splitted = split(locknode_with_seq, '/');

    while (true) {
        std::vector<std::string> children;
        if (!zkWrapper.get_children(my_locknode, children, error_code)) {
            return error_code;
        }
        std::sort(children.begin(), children.end());
        auto it = std::find(children.begin(), children.end(), splitted[splitted.size() - 1]);
        auto eq = splitted[splitted.size() - 1].compare(children[0]);
        int index;
        if (it == children.end()) {
            LOG(ERROR) << CLASS_NAME <<  "Failed to find " << splitted[splitted.size() - 1] << " in children!" << std::endl;
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
        std::string currentLockOwner = my_locknode + "/" + children[0];
        if (!zkWrapper.wexists(currentLockOwner, exists, notify, &cv, error_code)) {
            LOG(ERROR) << CLASS_NAME <<  "ZKLock::lock(): Failed to call wexists for " << currentLockOwner;
            return error_code;
        }

        while (exists){
            cv.wait_for(lck , std::chrono::milliseconds(100));
            if (!zkWrapper.wexists(currentLockOwner, exists, notify, &cv, error_code)) {
                LOG(ERROR) << CLASS_NAME <<  "ZKLock::lock(): Failed to call wexists for " << currentLockOwner;
                return error_code;
            }
        }
    }
}

int ZKLock::unlock(){
    // TODO: Possibly pass in an error_code& so that we can let the calling user know how it failed
    int error_code;
    if (locknode_with_seq.size() == 0){
        return -1;
    }
    zkWrapper.delete_node(locknode_with_seq, error_code);
    return error_code;
}