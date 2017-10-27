// Copyright 2017 Rice University, COMP 413 2017

#include <unistd.h>
#include <zookeeper.h>
#include <utility>
#include <string>
#include <vector>

#include "zkwrapper.h"

using std::clock;

#pragma once

typedef struct {
  uint32_t ipcPort;
  uint32_t xferPort;
  uint64_t disk_bytes;    // total space on disk
  uint64_t free_bytes;    // free space on disk
  uint32_t xmits;            // current number of xmits
} DataNodePayload;

class StorageMetrics {
 public:
  /**
   * Constructor
   * @param numDatanodes_ the number of datanodes, known by the caller test.
   *        This can be changed to be variable size later if needed.
   * @param zkWrapper_ the zkWrapper
   */
  explicit StorageMetrics(std::shared_ptr<ZKWrapper> zkWrapper_);

  /**
   * Returns the proportion of space used.
   * @return totalUsedSpace / totalSpace
   */
  float usedSpaceFraction();

  /**
   * Returns the total space used.
   * @return totalUsedSpace
   */
  float usedSpace();

  /**
   * Counts number of used blocks on each datanode, and takes the
   * standard deviation of that distribution.
   * @return The standard deviation of the blocks per datanode
   */
  float blocksPerDataNodeSD();

  /**
   * Measures the time for recovering from a DataNode failure.
   *
   * This method polls the zookeeper work_queues/replication until there are
   * none left. This should only be called from tests with a controlled number
   * of replications.
   *
   * Prints wall clock time spent in this function using easylogging++
   *
   * Logging level must be set to 9 before calling this function.
   *
   * @return 0 success, -1 error
   */
  int replicationRecoverySpeed();

  /**
   * watcher function to set on /work_queues/replicate.
   *
   * @param zzh unused
   * @param type unused
   * @param state unused
   * @param path Path to the znode to watch.
   * @param watcherCtx  "this"
   */
  static void watcher_replicate(zhandle_t *zzh,
                                int type,
                                int state,
                                const char *path,
                                void *watcherCtx);

  /**
   * Measures how long a read takes while a data block is being recovered.
   *
   * Prints the resulting time.
   *
   * Note that files under replication have no degenerate read. The degenerate
   *    case is the file being unreadable.
   * Files under EC must have a downed data block (not parity block) for the
   *    degenerate read case. Keep that in mind when passing in target DataNodes
   *
   * Logging level must be set to 9 before calling this function.
   *
   * @param file The file path to read.
   * @param destination The file name for writing cat output
   * @param targetDatanodes
   *        first: unix process name of a datanode to kill
   *        second: cliArgs for restarting that datanode.
   * @return 0 on success, 1 on error
   */
  float degenerateRead(
      std::string file,
      std::string destination,
      std::vector<std::pair<std::string, std::string>> targetDatanodes);

 private:
  uint64_t kNumDatanodes;
  std::shared_ptr<ZKWrapper> zkWrapper;

  clock_t tempClock;

  /**
   * Takes an array of integers and returns the standard deviation.
   * @param usedBlockCounts int array of length numDatanodes
   */
  float stDev(int usedBlockCounts[]);
};
