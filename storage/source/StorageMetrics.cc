// Copyright 2017 Rice University, COMP 413 2017

#include <StorageMetrics.h>
#include <unordered_map>
#include <boost/timer/timer.hpp>

float StorageMetrics::usedSpaceFraction() {
  uint64_t numerator = 0;
  uint64_t denominator = 0;
  int error = 0;
  std::vector<std::string> datanodeIds;
  if (!zkWrapper->get_children("/health", datanodeIds, error)) {
    LOG(ERROR) << "Failed to get /health children";
  }
  for (std::string &datanodeId : datanodeIds) {
    std::string statsPath = "/health/" + datanodeId + "/stats";
    std::vector<std::uint8_t> statsPayload = std::vector<std::uint8_t>();
    statsPayload.resize(sizeof(DataNodePayload));
    if (!zkWrapper->get(statsPath, statsPayload, error)) {
      LOG(ERROR) << "Failed to get " << statsPath;
      return 0;
    }
    DataNodePayload stats = DataNodePayload();
    memcpy(&stats, &statsPayload[0], sizeof(DataNodePayload));
    numerator += stats.disk_bytes - stats.free_bytes;
    denominator += stats.disk_bytes;
  }
  return (static_cast<float>(numerator)) / (static_cast<float>(denominator));
}

float StorageMetrics::blocksPerDataNodeSD() {
  int dataNodeBlockCounts[kNumDatanodes];
  std::fill_n(dataNodeBlockCounts, kNumDatanodes, 0);

  int error = 0;
  std::vector<std::string> datanode_ids;
  if (!zkWrapper->get_children("/health", datanode_ids, error)) {
    LOG(ERROR) << "Failed to get /health children";
  }

  int i = 0;
  for (std::string &datanode_id : datanode_ids) {
    std::string block_path = "/health/" + datanode_id + "/blocks";
    std::vector<std::string> block_ids;
    if (!zkWrapper->get_children(block_path, block_ids, error)) {
      LOG(ERROR) << "Failed to get " << block_path;
    }
    dataNodeBlockCounts[i++] = static_cast<int>(block_ids.size());
  }

  return stDev(dataNodeBlockCounts);
}

float StorageMetrics::recoverySpeed() {
  // TODO(ejd6): implement
  return 0.0;
}

float StorageMetrics::degenerateRead(
    std::string file,
    std::string destination,
    std::vector<std::pair<std::string, std::string>> targetDatanodes) {
  // Kill the datanodes.
  for (std::pair<std::string, std::string> datanode : targetDatanodes) {
    system("pkill -f " + datanode.first);
    sleep(5);
  }

  boost::timer::cpu_times elapsed;
  // Timer measures wall clock and CPU time.
  boost::timer::cpu_timer timer;

  // Do the read.
  int status = system("hdfs dfs -fs hdfs://localhost:5351 -cat "
                          + file
                          + " > "
                          + destination);
  if (status < 0) {
    LOG(ERROR) << "degenerateRead Error: " << strerror(errno);
  } else {
    if (WIFEXITED(status)) {
      // Successful return.
      elapsed = timer.elapsed();
    } else {
      LOG(ERROR) << "degenerateRead error: Program exited abnormally";
    }
  }

  // Restart the datanodes so this function has less "side effects".
  for (std::pair<std::string, std::string> datanode : targetDatanodes) {
    std::string cmdLine =
        "bash -c \"exec -a " + datanode.first +
            " /home/vagrant/rdfs/build/rice-datanode/datanode " +
            datanode.second + "\" & ";
    system(cmdLine.c_str());
    sleep(3);
  }

  return static_cast<float>(elapsed.wall / 1e9);
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
