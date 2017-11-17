// Copyright 2017 Rice University, COMP 413 2017

#define ELPP_THREAD_SAFE

#include <easylogging++.h>
#include <gtest/gtest.h>
#include <iostream>

#include "rpcserver.h"
#include "ClientNamenodeProtocolImpl.h"
#include "zkwrapper.h"
#include "zk_nn_client.h"

INITIALIZE_EASYLOGGINGPP

using client_namenode_translator::ClientNamenodeTranslator;

namespace {

    class ACLTest : public ::testing::Test {
    protected:
        virtual void SetUp() {

            int error_code;
            zk = std::make_shared<ZKWrapper>("localhost:2181", error_code, "/testing");
            assert(error_code == 0);  // Z_OK

            nncli = new zkclient::ZkNnClient(zk);
            nncli->register_watches();
            nn_translator = new ClientNamenodeTranslator(5351, nncli);

        }
        // Objects declared here can be used by all tests in the test case for Foo.
        zkclient::ZkNnClient *client;
        zkclient::ZkNnClient *nncli;
        ClientNamenodeTranslator *nn_translator;
        std::shared_ptr<ZKWrapper> zk;


        hadoop::hdfs::CreateRequestProto getCreateRequestProto(
                const std::string &path, const std::string &user)
        {
            hadoop::hdfs::CreateRequestProto create_req;
            create_req.set_src(path);
            create_req.set_clientname(user);
            create_req.set_createparent(false);
            create_req.set_blocksize(1);
            create_req.set_replication(1);
            create_req.set_createflag(0);
            return create_req;
        }
    };

TEST_F(ACLTest, testReadOwnFile) {

// Make a file.
ASSERT_EQ(0,
system(
"python /home/vagrant/rdfs/test/integration/generate_file.py > "
"expected_testfile1234"));
// Put it into rdfs.
system(
"hdfs dfs -fs hdfs://localhost:5351 "
"-copyFromLocal expected_testfile1234 /f");
// Read it from rdfs.
system("hdfs dfs -fs hdfs://localhost:5351 -cat /f > actual_testfile1234");
// Check that its contents match.
ASSERT_EQ(0,
system("diff expected_testfile1234 actual_testfile1234 > "
"/dev/null"));
system("hdfs dfs -fs hdfs://localhost:5351 -rm /f");

}


TEST_F(ACLTest, testUnauthorizedReadFailFile) {

// Make a file.
ASSERT_EQ(0,
system(
"python /home/vagrant/rdfs/test/integration/generate_file.py > "
"expected_testfile1234"));

// Put it into rdfs.
system(
"hdfs dfs -fs hdfs://localhost:5351 "
"-copyFromLocal expected_testfile1234 /f");

// Switch users.
system("sudo su - user2");

system("hdfs dfs -fs hdfs://localhost:5351 -cat /f > actual_testfile1234");

// Check that its contents do not match.
ASSERT_NE(0,
system("diff expected_testfile1234 actual_testfile1234 > "
"/dev/null"));
system("hdfs dfs -fs hdfs://localhost:5351 -rm /f");

}

TEST_F(ACLTest, testAddPermFile) {

std::string oldUsername;
oldUsername = getenv("USER");

// Make a file.
ASSERT_EQ(0,
system(
"python /home/vagrant/rdfs/test/integration/generate_file.py > "
"expected_testfile1234"));

// Put it into rdfs.
system(
"hdfs dfs -fs hdfs://localhost:5351 "
"-copyFromLocal expected_testfile1234 /f");

// Add permissions for user2
system( "hdfs dfs -fs hdfs://localhost:5351 "
"-chmod 755 user2");

// Switch users.
system("sudo su - user2");

system("hdfs dfs -fs hdfs://localhost:5351 -cat /f > actual_testfile1234");

// Check that its contents match.
ASSERT_EQ(0,
system("diff expected_testfile1234 actual_testfile1234 > "
"/dev/null"));
system("hdfs dfs -fs hdfs://localhost:5351 -rm /f");

}


TEST_F(ACLTest, testRemovePermFile) {

std::string oldUsername;
oldUsername = getenv("USER");

// Make a file.
ASSERT_EQ(0,
system(
"python /home/vagrant/rdfs/test/integration/generate_file.py > "
"expected_testfile1234"));

// Put it into rdfs.
system(
"hdfs dfs -fs hdfs://localhost:5351 "
"-copyFromLocal expected_testfile1234 /f");

// Add permissions for user2
system( "hdfs dfs -fs hdfs://localhost:5351 "
"-chmod 755 user2");

// Switch users.
system("sudo su - user2");

system("hdfs dfs -fs hdfs://localhost:5351 -cat /f > actual_testfile1234");

// Check that its contents match.
ASSERT_EQ(0,
system("diff expected_testfile1234 actual_testfile1234 > "
"/dev/null"));
system("hdfs dfs -fs hdfs://localhost:5351 -rm /f");

// Switch back to first user
std::string str = "sudo su - " + oldUsername;
system(str.c_str());

// Remove permissions for user2
system( "hdfs dfs -fs hdfs://localhost:5351 "
"-chmod 700 user2");

// switch back to second user
system("sudo su - user2");

system("hdfs dfs -fs hdfs://localhost:5351 -cat /f > actual_testfile1234");

// Check that its contents do not match.
ASSERT_NE(0,
system("diff expected_testfile1234 actual_testfile1234 > "
"/dev/null"));
system("hdfs dfs -fs hdfs://localhost:5351 -rm /f");

// Switch users again.
system("echo \"vagrant\" > in");
system("sudo su - user2");
// hdfs dfs chmod 755 <filename>

}


}  // namespace


int main(int argc, char **argv) {

    system("sudo /usr/sbin/deluser user2");
    system("sudo /usr/sbin/adduser user2 --gecos \"F,R,W,H\" --disabled-password");
    system("echo \"user2:vagrant\" | sudo /usr/sbin/chpasswd");

//        system("sudo adduser user2");
    system("sudo /usr/sbin/usermod -aG sudo user2");

    // Start up zookeeper
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh start");
  sleep(10);

  system("/home/vagrant/rdfs/build/rice-namenode/namenode &");
  system("/home/vagrant/rdfs/build/rice-datanode/datanode &");
  // Initialize and run the tests
  ::testing::InitGoogleTest(&argc, argv);
  int res = RUN_ALL_TESTS();

  // Remove test files and shutdown zookeeper
  sleep(10);
  system("~/zookeeper/bin/zkCli.sh rmr /testing");
  system("pkill -f namenode");
  system("pkill -f datanode");
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");

 // std::cin.rdbuf(cinbuf);   //reset to standard input again

  return res;
}
