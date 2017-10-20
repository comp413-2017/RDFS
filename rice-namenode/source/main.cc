// Copyright 2017 Rice University, COMP 413 2017

#include <rpcserver.h>
#include <easylogging++.h>
#include <cstdlib>
#include <iostream>
#include <asio.hpp>
#include "ClientNamenodeProtocolImpl.h"
#include "HaServiceProtocolImpl.h"
#include "zk_nn_client.h"

#define ELPP_FRESH_LOG_FILE
#define ELPP_THREAD_SAFE

INITIALIZE_EASYLOGGINGPP

// Absolute directory from vagrant VM to config file
#define LOG_CONFIG_FILE "/home/vagrant/rdfs/config/nn-log-conf.conf"

using client_namenode_translator::ClientNamenodeTranslator;

static bool admin = true;

/**
 * Function to parse commandline options and store results into the input
 * pointers.
 * @param argc Count of arguments received at the command line.
 * @param argv Array of character arrays storing arguments.
 * @param xferPort A pointer in which to place the xferPort entered at the
 * command line.
 * @param ipcPort A pointer in which to place the ipcPort entered at the
 * command line.
 * @param verbosity A pointer in which to place the verbosity value entered at
 * the command line.
 * @param node_policy A pointer in which to place the node_policy value entered at the command line.
 * @param backingStore A reference to set to the input backingStore.
 * @return 0 on success, -1 on any error.
 */
static inline int parse_cmdline_options(
    int argc,
    char *argv[],
    int *port,
    int *verbosity,
    char *node_policy
) {
  int c;
  char buf[64];

  // By setting opterr to 0, getopt does not print its own error messages.
  opterr = 0;

  // We expect to find port, node policy, and/or verbosity settings
  while ((c = getopt(argc, argv, "v:p:n:")) != -1) {
    switch (c) {
      case 'v':*verbosity = atoi(optarg);
        if (*verbosity < 0 || *verbosity > 9) {
          LOG(ERROR) << "Verbosity must be between 0 and 9.";
          return -1;
        }
        break;
      case 'p':*port = atoi(optarg);
        if (*port < 0) {
          LOG(ERROR) << "NameNode IPC port must be greater than 0.";
          return -1;
        }
        break;
      case 'n':*node_policy = optarg[0];
        if (*node_policy != MIN_XMITS || *node_policy != MAX_FREE_SPACE) {
          LOG(ERROR) << "Node policy, if any, must be -n x (minimum transmits)"
                        " or -n f (maximum free space)";
          return -1;
        }
        break;
      case '?':
        switch (optopt) {
          case 'v':LOG(ERROR) << "Option -v requires an argument specifying a "
                "verbosity level.";
            return -1;
          case 'p':LOG(ERROR) << "Option -p requires an argument specifying an "
                "ipc port.";
            return -1;
          case 'n':LOG(ERROR) << "Option -n requires an argument specifying a "
                 "node policy.";
            return -1;
          default:
            if (isprint(optopt)) {
              snprintf(buf, sizeof(buf), "Unknown option -%c.", optopt);
              LOG(WARNING) << buf;
              return 0;
            } else {
              snprintf(
                  buf,
                  sizeof(buf),
                  "Unknown option character `\\\\x%x'.",
                  optopt);
              LOG(WARNING) << buf;
              return 0;
            }
        }
        break;
      default:return -1;
    }
  }

  return 0;
}

int main(int argc, char *argv[]) {
  el::Configurations conf(LOG_CONFIG_FILE);
  el::Loggers::reconfigureAllLoggers(conf);
  el::Loggers::addFlag(el::LoggingFlag::ColoredTerminalOutput);
  el::Loggers::addFlag(el::LoggingFlag::LogDetailedCrashReason);

  int error_code = 0;

  asio::io_service io_service;
  int port = 5351;
  int verbosity = 0;
  char node_policy = MAX_FREE_SPACE;

  parse_cmdline_options(argc, argv, &port, &verbosity, &node_policy);

  el::Loggers::setVerboseLevel(verbosity);

  auto zk_shared = std::make_shared<ZKWrapper>(
      "localhost:2181,localhost:2182,localhost:2183",
      error_code,
      "/testing");
  zkclient::ZkNnClient nncli(zk_shared);
  nncli.register_watches();
  nncli.set_node_policy(node_policy);

  LOG(INFO) << "Namenode is starting";
  ClientNamenodeTranslator translator(port, &nncli);
  // high availability translator
  RPCServer server = translator.getRPCServer();
  ha_service_translator::HaServiceTranslator ha_service_translator(
      &server,
      &nncli,
      port);
  server.serve(io_service);

  if (admin) {
    const std::string zk_admin_path = "/security/metadata/admin";
    zk_shared.get()->create(zk_admin_path,
                            ZKWrapper::get_byte_vector(server.getUsername()),
                            error_code,
                            false,
                            true);
    admin = false;
  }
}
