// Copyright 2017 Rice University, COMP 413 2017
// Created by Nicholas Kwon on 10/15/16.
//

#ifndef ZKWRAPPER_INCLUDE_ZK_LOCK_H_
#define ZKWRAPPER_INCLUDE_ZK_LOCK_H_

#include <easylogging++.h>
#include <algorithm>
#include <thread>              // std::thread, std::this_thread::yield
#include <mutex>               // std::mutex, std::unique_lock
#include <condition_variable>  // std::condition_variable
#include <string>
#include <sstream>
#include <vector>
#include <boost/algorithm/string.hpp>
#include "zkwrapper.h"

/**
 * ZKLock is a globally synchronous lock that is implemented using ZooKeeper.
 * Multiple clients can synchronize on a resource by locking onto the same path.
 */
class ZKLock {
 public:
  /**
   *
   * @param zkWrapper
   * @param path the path to lock onto. This path does not need to exist inside ZooKeeper.
   * @return
   */
  ZKLock(ZKWrapper &zkWrapper, std::string path) :
          zkWrapper(zkWrapper), path_to_lock(path) {}

  /**
   * Blocks until the thread is able to lock onto the path.
   * @return a negative value if an error occurs; 0 otherwise
   */
  int lock();

  /**
   * @return a negative value if an error occurs; 0 otherwise
   */
  int unlock();

 private:
  ZKWrapper zkWrapper;

  std::string path_to_lock;

  static const char lock_path[];

  std::string locknode_with_seq;

  static const std::string CLASS_NAME;
};
#endif  // ZKWRAPPER_INCLUDE_ZK_LOCK_H_
