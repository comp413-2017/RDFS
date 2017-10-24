// Copyright 2017 Rice University, COMP 413 2017

#ifndef TEST_UTIL_RDFSTESTUTILS_H_
#define TEST_UTIL_RDFSTESTUTILS_H_

#include <string>

namespace RDFSTestUtils {

  static void initializeDatanodes(
      int startId,
      int numDatanodes,
      std::string serverName,
      int32_t xferPort,
      int32_t ipcPort) {
    int i = startId;
    for (; i < startId + numDatanodes; i++) {
      system(("truncate tfs" + std::to_string(i) + " -s 1000000000").c_str());
      std::string dnCliArgs = "-x " +
          std::to_string(xferPort + i) + " -p " + std::to_string(ipcPort + i)
          + " -b tfs" + std::to_string(i) + " &";
      std::string cmdLine =
          "bash -c \"exec -a " + serverName + std::to_string(i) +
              " /home/vagrant/rdfs/build/rice-datanode/datanode " +
              dnCliArgs + "\" & ";
      system(cmdLine.c_str());
      sleep(3);
    }
  }
}  // namespace RDFSTestUtils

#endif  // TEST_UTIL_RDFSTESTUTILS_H_
