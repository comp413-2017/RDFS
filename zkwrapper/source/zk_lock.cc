// Copyright 2017 Rice University, COMP 413 2017
// Created by Nicholas Kwon on 10/15/16.
//
#include "zk_lock.h"

const char ZKLock::lock_path[] = "/_locknode_";

int ZKLock::lock() {
  std::mutex mtx;
  std::condition_variable cv;
  std::unique_lock<std::mutex> lck(mtx);

  std::replace(path_to_lock.begin(), path_to_lock.end(), '/', ':');

  bool exists;
  int error_code;

  std::string my_locknode(std::string(lock_path) + "/" + path_to_lock);
  if (!zkWrapper.exists(my_locknode, exists, error_code)) {
    LOG(ERROR) << "ZKLock::lock(): Failed to check existence for "
               << my_locknode;
    return error_code;
  }
  if (!exists && !zkWrapper.recursive_create(my_locknode,
                                             ZKWrapper::EMPTY_VECTOR,
                                             error_code,
                                             false)) {
    LOG(ERROR) << "ZKLock::lock(): Failed to recursively create "
               << my_locknode;
    return error_code;
  }
  std::string my_lock(my_locknode + "/lock-");
  if (!zkWrapper.create_sequential(my_lock,
                                   ZKWrapper::EMPTY_VECTOR,
                                   locknode_with_seq,
                                   true,
                                   error_code,
                                   false)) {
    LOG(ERROR) << "ZKLock::lock(): Failed to create sequential " << my_lock;
    return error_code;
  }
  std::vector<std::string> splitted;
  boost::split(splitted, locknode_with_seq, boost::is_any_of("/"));

  while (true) {
    std::vector<std::string> children;
    if (!zkWrapper.get_children(my_locknode, children, error_code)) {
      return error_code;
    }
    std::sort(children.begin(), children.end());
    auto it = std::find(children.begin(),
                        children.end(),
                        splitted[splitted.size() - 1]);
    auto eq = splitted[splitted.size() - 1].compare(children[0]);
    int index;
    if (it == children.end()) {
      LOG(ERROR) << "Failed to find "
                 << splitted[splitted.size() - 1]
                 << " in children!"
                 << std::endl;
      return -1;
    } else {
      index = std::distance(children.begin(), it);
    }
    if (index == 0) {
      return 0;
    }
    auto notify = [](zhandle_t *zzh,
                     int type,
                     int state,
                     const char *path,
                     void *watcherCtx) {
      auto cvar = reinterpret_cast<std::condition_variable *>(watcherCtx);
      cvar->notify_all();
    };
    std::string currentLockOwner = my_locknode + "/" + children[0];
    if (!zkWrapper.wexists(currentLockOwner, exists, notify, &cv, error_code)) {
      LOG(ERROR) << "ZKLock::lock(): Failed to call wexists for "
                 << currentLockOwner;
      return error_code;
    }

    while (exists) {
      cv.wait_for(lck, std::chrono::milliseconds(100));
      if (!zkWrapper.wexists(currentLockOwner,
                             exists,
                             notify,
                             &cv,
                             error_code)) {
        LOG(ERROR) << "ZKLock::lock(): Failed to call wexists for "
                   << currentLockOwner;
        return error_code;
      }
    }
  }
}

int ZKLock::unlock() {
  int error_code;
  if (locknode_with_seq.size() == 0) {
    return -1;
  }
  zkWrapper.delete_node(locknode_with_seq, error_code);
  return error_code;
}
