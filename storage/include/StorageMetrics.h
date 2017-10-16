// Copyright 2017 Rice University, COMP 413 2017

#include <zookeeper.h>

#include "zkwrapper.h"

#pragma once

class StorageMetrics {
 public:
  /**
   * Constructor
   * @param numDatanodes_ the number of datanodes, known by the caller test.
   */
  explicit StorageMetrics(
      int numDatanodes_,
      std::shared_ptr<ZKWrapper> zkWrapper_) :
        kNumDatanodes(numDatanodes_),
        zkWrapper(zkWrapper_) {}

  /**
   * Measures sum of used space on all DataNodes
   * @return total used space (TODO: units???)
   */
  int totalUsedSpace();

  /**
   * Measures sum of sizes of DataNodes.
   * @return total space in all DataNodes (TODO: units???)
   */
  int totalSpace();

  /**
   * Returns the proportion of space used.
   * @return totalUsedSpace / totalSpace
   */
  float usedSpaceFraction();

  /**
   * Counts number of used blocks on each datanode, and takes the
   * standard deviation of that distribution.
   * @return The standard deviation of the blocks per datanode
   */
  float blocksPerDataNodeSD();

  /**
   * Measures the time for recovering from a DataNode failure.
   * @return wall clock time from failure to being prepared for another failure.
   */
  float recoverySpeed();

  /**
   * Measures how long a read takes while a desired block is being recovered.
   * @return wall clock time for the read
   */
  float degenerateRead();

 private:
  int kNumDatanodes;
  std::shared_ptr<ZKWrapper> zkWrapper;

  /**
   * Takes an array of integers and returns the standard deviation.
   * @param usedBlockCounts int array of length numDatanodes
   */
  float stDev(int usedBlockCounts[]);
};
