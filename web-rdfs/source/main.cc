// Copyright 2017 Rice University, COMP 413 2017

#include "web_rdfs_server.h"
#include <cstdlib>
#include <easylogging++.h>

#define ELPP_FRESH_LOG_FILE
#define ELPP_THREAD_SAFE

INITIALIZE_EASYLOGGINGPP

#define LOG_CONFIG_FILE "/home/vagrant/rdfs/config/nn-log-conf.conf"

static inline void parse_cmdline_options(int argc,
                                         char *argv[],
                                         int16_t *port) {
  int c;
  char buf[64];

  opterr = 0;

  while ((c = getopt(argc, argv, "p:")) != -1) {
    switch (c) {
      case 'p':
        *port = atoi(optarg);
        if (*port < 0) {
          LOG(ERROR) << "WebRDFS port must be greater than 0.";
          return;
        }
        break;
      case '?':
        switch (optopt) {
          case 'p':
            LOG(ERROR) << "Option -p requires specifying an HTTP port";
            return;
          default:
            if (isprint(optopt)) {
              snprintf(buf, sizeof(buf), "Unknown option -%c.", optopt);
              LOG(WARNING) << buf;
              return;
            } else {
              snprintf(
                buf,
                sizeof(buf),
                "Unknown option character `\\\\x%x'.",
                optopt);
              LOG(WARNING) << buf;
              return;
            }
        }
      default:
        return;
    }
  }
}

int main(int argc, char *argv[]) {
  el::Configurations conf(LOG_CONFIG_FILE);
  el::Loggers::reconfigureAllLoggers(conf);
  el::Loggers::addFlag(el::LoggingFlag::ColoredTerminalOutput);
  el::Loggers::addFlag(el::LoggingFlag::LogDetailedCrashReason);

  int16_t port = 8080;
  parse_cmdline_options(argc, argv, &port);

  WebRDFSServer server(port);
  server.start();
}
