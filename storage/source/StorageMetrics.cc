// Copyright 2017 Rice University, COMP 413 2017

#include <gtest/gtest.h>
#include <unordered_map>
#include <StorageMetrics.h>

float StorageMetrics::usedSpaceFraction() {
  int64_t numerator = 0;
  int64_t denominator = 0;
  int error = 0;
  std::vector<std::string> datanodeIds;
  zkWrapper->get_children("/health", datanodeIds, error);
  for (std::string &datanodeId : datanodeIds) {
    std::string statsPath = "/health/" + datanodeId + "/stats";
    std::vector<std::uint8_t> statsPayload = std::vector<std::uint8_t>();
    statsPayload.resize(sizeof(DataNodePayload));
    if (!zkWrapper->get(statsPath, statsPayload, error)) {
      LOG(ERROR) << "Failed to get " << statsPath;
      return -1;
    }
    DataNodePayload stats = DataNodePayload();
    memcpy(&stats, &statsPayload[0], sizeof(DataNodePayload));
    numerator += stats.disk_bytes - stats.free_bytes;
    denominator += stats.disk_bytes;
  }
  LOG(INFO) << "----- USED BYTES: " << numerator;
  LOG(INFO) << "----- total BYTES: " << denominator;
  return (static_cast<float>(numerator)) / (static_cast<float>(denominator));
}

float StorageMetrics::blocksPerDataNodeSD() {
  int dataNodeBlockCounts[kNumDatanodes];
  std::fill_n(dataNodeBlockCounts, kNumDatanodes, 0);

  int error = 0;
  std::vector<std::string> datanode_ids;
  zkWrapper->get_children("/health", datanode_ids, error);

  int i = 0;
  for (std::string &datanode_id : datanode_ids) {
    std::string block_path = "/health/" + datanode_id + "/blocks";
    std::vector<std::string> block_ids;
    zkWrapper->get_children(block_path, block_ids, error);
    dataNodeBlockCounts[i++] = static_cast<int>(block_ids.size());
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
