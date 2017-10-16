#ifndef RDFS_NAMENODE_TEST_H
#define RDFS_NAMENODE_TEST_H

#define ELPP_FRESH_LOG_FILE
#define ELPP_THREAD_SAFE

#include <easylogging++.h>
#include <gtest/gtest.h>
#include <cstring>
#include <ctime>
#include "zkwrapper.h"
#include "zk_nn_client.h"
#include "zk_dn_client.h"

#define LOG_CONFIG_FILE "/home/vagrant/rdfs/config/test-log-conf.conf"

class NamenodeTest : public ::testing::Test {
protected:
    virtual void SetUp() {
        int error_code;
        auto zk_shared =
                std::make_shared<ZKWrapper>("localhost:2181", error_code, "/testing");
        assert(error_code == 0);  // Z_OK
        client = new zkclient::ZkNnClient(zk_shared);
        zk = new ZKWrapper("localhost:2181", error_code, "/testing");
    }

    hadoop::hdfs::CreateRequestProto getCreateRequestProto(
            const std::string &path
    ) {
        hadoop::hdfs::CreateRequestProto create_req;
        create_req.set_src(path);
        create_req.set_clientname("asdf");
        create_req.set_createparent(false);
        create_req.set_blocksize(1);
        create_req.set_replication(1);
        create_req.set_createflag(0);
        return create_req;
    }

    // Objects declared here can be used by all tests in the test case for Foo.
    ZKWrapper *zk;
    zkclient::ZkNnClient *client;
};

#endif // RDFS_NAMENODE_TEST_H
