// Copyright 2017 Rice University, COMP 413 2017

#include <gtest/gtest.h>
#include <cstdlib>
#include <cmath>
#include "StorageMetrics.h"

int StorageMetrics::totalUsedSpace() {
  // TODO: use zkwrapper to read contents of <zkRootPrefix>/health/datanodes/*/blocks ?? or just block_locations
  return 0;
}

int StorageMetrics::totalSpace() {
  // TODO: use zkwrapper to read contents of <zkRootPrefix>/health/datanodes/*/blocks ?? or just block_locations
  return 0;
}

float StorageMetrics::usedSpaceFraction() {
  return static_cast<float>(totalUsedSpace()) /
      static_cast<float>(totalSpace());
}

float StorageMetrics::blocksPerDataNodeSD() {
  int dataNodeBlockCounts[kNumDatanodes];
  std::fill_n(dataNodeBlockCounts, kNumDatanodes, 0);

  int error = 0;

  std::vector<std::string> datanode_ids;
  zkWrapper->get_children("/health/", datanode_ids, error);
  LOG(INFO) << "DATANODE COUNT: " << datanode_ids.size();

  int i = 0;
  for (std::string &datanode_id : datanode_ids) {
    std::string block_path = "/health/" + datanode_id + "/blocks";
    std::vector<std::string> block_ids;
    zkWrapper->get_children(block_path, block_ids, error);
    LOG(INFO) << "DATANODE ID: " << datanode_id;
    LOG(INFO) << "BLOCK COUNT: " << block_ids.size();
    dataNodeBlockCounts[i] = static_cast<int>(block_ids.size());
  }

  return stDev(dataNodeBlockCounts);
}

float StorageMetrics::recoverySpeed() {
  // TODO(ejd6): implement
  return 0.0;
}

float StorageMetrics::degenerateRead() {
  // TODO(ejd6): implement
  return 0.0;
}

float StorageMetrics::stDev(int usedBlockCounts[]) {
  float standardDeviation = 0.0;
  float sum = 0.0;
  float mean;
  int i;

  for (i = 0; i < kNumDatanodes; i++) {
    sum += usedBlockCounts[i];
  }

  mean = sum / kNumDatanodes;

  for (i = 0; i < kNumDatanodes; i++) {
    standardDeviation += pow(usedBlockCounts[i] - mean, 2);
  }

  return sqrtf(standardDeviation / kNumDatanodes);
}
