//
// Created by Nicholas Kwon on 10/15/16.
//

#ifndef RICE_HDFS_ZK_LOCK_H_H
#define RICE_HDFS_ZK_LOCK_H_H

#include <algorithm>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <thread>             // std::thread, std::this_thread::yield
#include <mutex>              // std::mutex, std::unique_lock
#include <condition_variable> // std::condition_variable

#include "zkwrapper.h"

class ZKLock{
public:
    ZKLock(ZKWrapper &zkWrapper, std::string path) : zkWrapper(zkWrapper), path_to_lock(path) {}

    /**
     * Locks
     * @param path
     */
    int lock();

    int unlock();

private:
    ZKWrapper zkWrapper;

    std::string path_to_lock;

    static const std::string lock_path;

    static std::string generate_uuid();

    std::string locknode_with_seq;
};
#endif //RICE_HDFS_ZK_LOCK_H_H
