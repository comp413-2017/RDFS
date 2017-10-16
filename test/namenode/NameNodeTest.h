#include <easylogging++.h>

#include <gtest/gtest.h>
#include <cstring>
#include <ctime>
#include <chrono>
#include "zkwrapper.h"
#include "zk_nn_client.h"
#include "zk_dn_client.h"

#define ELPP_THREAD_SAFE

#define LOG_CONFIG_FILE "/home/vagrant/rdfs/config/test-log-conf.conf"

class NamenodeTest : public ::testing::Test {
protected:
    void SetUp();

    hadoop::hdfs::CreateRequestProto getCreateRequestProto(
            const std::string &path
    );

    // Objects declared here can be used by all tests in the test case for Foo.
    ZKWrapper *zk;
    zkclient::ZkNnClient *client;
};
