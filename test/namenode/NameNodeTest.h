// Copyright 2017 Rice University, COMP 413 2017

#ifndef TEST_NAMENODE_NAMENODETEST_H_
#define TEST_NAMENODE_NAMENODETEST_H_

#include <easylogging++.h>

#include <gtest/gtest.h>
#include <cstring>
#include <string>
#include <ctime>
#include "zkwrapper.h"
#include "zk_nn_client.h"
#include "zk_dn_client.h"

#define ELPP_THREAD_SAFE

#define LOG_CONFIG_FILE "/home/vagrant/rdfs/config/test-log-conf.conf"

class NamenodeTest : public ::testing::Test {
 protected:
    void SetUp();

    hadoop::hdfs::CreateRequestProto getCreateRequestProto(
            const std::string &path);

    // Objects declared here can be used by all tests in the test case for Foo.
    ZKWrapper *zk;
    zkclient::ZkNnClient *client;
};

#endif  // TEST_NAMENODE_NAMENODETEST_H_
