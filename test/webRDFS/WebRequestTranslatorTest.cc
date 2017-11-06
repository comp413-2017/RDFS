// Copyright 2017 Rice University, COMP 413 2017

#define ELPP_THREAD_SAFE

#include <easylogging++.h>

#include <gtest/gtest.h>
#include "webRequestTranslator.h"

#include <iostream>

INITIALIZE_EASYLOGGINGPP

namespace {
}

int main(int argc, char **argv) {
    // Start up zookeeper
    system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
    system("sudo /home/vagrant/zookeeper/bin/zkServer.sh start");
    sleep(10);

    // Initialize and run the tests
    ::testing::InitGoogleTest(&argc, argv);
    int res = RUN_ALL_TESTS();

    // Remove test files and shutdown zookeeper
    system("~/zookeeper/bin/zkCli.sh rmr /testing");
    system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
    return res;
}
