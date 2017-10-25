// Copyright 2017 Rice University, COMP 413 2017

#include <easylogging++.h>
#include <rpcserver.h>

#include <cstdlib>
#include <iostream>
#include <thread>

#include <asio.hpp>
#include "ClientDatanodeProtocolImpl.h"
#include "DaemonFactory.h"
#include "data_transfer_server.h"
#include "native_filesystem.h"
#include "zk_dn_client.h"

#define ELPP_FRESH_LOG_FILE
#define ELPP_THREAD_SAFE

// initialize the logging library (only do this once!)
INITIALIZE_EASYLOGGINGPP

// Absolute directory from vagrant VM to config file
#define LOG_CONFIG_FILE "/home/vagrant/rdfs/config/dn-log-conf.conf"

/**
 * Function to parse commandline options and store results into the input
 * pointers.
 *
 * @param argc Count of arguments received at the command line.
 * @param argv Array of character arrays storing arguments.
 * @param xferPort A pointer in which to place the xferPort entered at the
 *                 command line.
 * @param ipcPort A pointer in which to place the ipcPort entered at the command
 *                line.
 * @param verbosity A pointer in which to place the verbosity value entered at
 *                  the command line.
 * @param backingStore A reference to set to the input backingStore.
 * @return 0 on success, -1 on any error.
 */
static inline int parse_cmdline_options(int argc, char *argv[], int *xferPort,
                                        int *ipcPort, int *verbosity,
                                        std::string &backingStore) {
  int c;
  char buf[64];

  // By setting opterr to 0, getopt does not print its own error messages.
  opterr = 0;

  // We expect to find port setting, verbosity setting, and/or backingStore
  // setting.
  while ((c = getopt(argc, argv, "v:p:x:b:")) != -1) {
    switch (c) {
      case 'v':
        *verbosity = atoi(optarg);
        if (*verbosity < 0 || *verbosity > 9) {
          LOG(ERROR) << "Verbosity must be between 0 and 9.";
          return -1;
        }
        break;
      case 'p':
        *ipcPort = atoi(optarg);
        if (*ipcPort < 0) {
          LOG(ERROR) << "DataNode IPC port must be greater than 0.";
          return -1;
        }
        break;
      case 'x':
        *xferPort = atoi(optarg);
        if (*xferPort < 0) {
          LOG(ERROR) << "DataNode TranferServer port must be greater than 0.";
          return -1;
        }
        break;
      case 'b':
        backingStore = std::string(optarg);
        break;
      case '?':
        switch (optopt) {
          case 'v':
            LOG(ERROR)
              << "Option -v requires an argument specifying a verbosity level.";
            return -1;
          case 'p':
            LOG(ERROR)
              << "Option -p requires an argument specifying an ipc port.";
            return -1;
          case 'x':
            LOG(ERROR)
              << "Option -x requires an argument specifying a transfer server "
                 "port.";
            return -1;
          case 'b':
            LOG(ERROR)
              << "Option -b requires an argument specifying the backing "
                 "storage device.";
            return -1;
          default:
            if (isprint(optopt)) {
              snprintf(buf, sizeof(buf), "Unknown option -%c.", optopt);
              LOG(WARNING) << buf;
              return 0;
            } else {
              snprintf(buf, sizeof(buf), "Unknown option character `\\\\x%x'.",
                       optopt);
              LOG(WARNING) << buf;
              return 0;
            }
        }
      default:
        return -1;
    }
  }

  return 0;
}

int main(int argc, char *argv[]) {
  el::Configurations conf(LOG_CONFIG_FILE);
  el::Loggers::reconfigureAllLoggers(conf);
  el::Loggers::addFlag(el::LoggingFlag::ColoredTerminalOutput);

  int error_code = 0;

  asio::io_service io_service;
  int xferPort = 50010;
  int ipcPort = 50020;
  int verbosity = 0;
  std::string backingStore("/dev/sdb");

  if (parse_cmdline_options(argc, argv, &xferPort, &ipcPort, &verbosity,
                            backingStore) != 0) {
    LOG(FATAL) << "Failed to parse expected command line arguments.";
    return -1;
  }

  el::Loggers::setVerboseLevel(verbosity);

  LOG(DEBUG) << "BackingStore set to " << backingStore;
  auto fs = std::make_shared<nativefs::NativeFS>(backingStore);
  if (fs == nullptr) {
    LOG(FATAL) << "Failed to create filesystem.";
    return -1;
  }

  uint64_t total_disk_space = fs->getTotalSpace();
  auto zk_shared = std::make_shared<ZKWrapper>(
    "localhost:2181,localhost:2182,localhost:2183", error_code, "/testing");
  auto dncli = std::make_shared<zkclient::ZkClientDn>("127.0.0.1",
                                                      zk_shared,
                                                      total_disk_space,
                                                      ipcPort,
                                                      // TODO(eddiedugan):
                                                      // Change the datanode id
                                                      xferPort);
  client_datanode_translator::ClientDatanodeTranslator translator(ipcPort);
  auto transfer_server = std::make_shared<TransferServer>(xferPort, fs, dncli);
  dncli->setTransferServer(transfer_server);
  daemon_thread::DaemonThreadFactory factory;
  factory.create_daemon_thread(&TransferServer::sendStats,
                               transfer_server.get(), 3);
  factory.create_daemon_thread(&TransferServer::poll_replicate,
                               transfer_server.get(), 2);
  factory.create_daemon_thread(&TransferServer::poll_delete,
                               transfer_server.get(), 5);
  std::thread(&TransferServer::serve, transfer_server.get(),
              std::ref(io_service)).detach();
  translator.getRPCServer().serve(io_service);
}
