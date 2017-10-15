// Copyright 2017 Rice University, COMP 413 2017

#define ELPP_THREAD_SAFE

#include <easylogging++.h>
#include <gtest/gtest.h>

#include "rpcserver.h"
#include "ClientNamenodeProtocolImpl.h"
#include "zkwrapper.h"
#include "zk_nn_client.h"
#include <iostream>

INITIALIZE_EASYLOGGINGPP

using client_namenode_translator::ClientNamenodeTranslator;

namespace {
TEST(UsernameTest, testGetUser) {
  zkclient::ZkNnClient *nncli;
  ClientNamenodeTranslator *nn_translator;
  std::shared_ptr<ZKWrapper> zk;

  int error_code;
  zk = std::make_shared<ZKWrapper>("localhost:2181", error_code, "/testing");
  assert(error_code == 0);  // Z_OK

  nncli = new zkclient::ZkNnClient(zk);
  nncli->register_watches();
  nn_translator = new ClientNamenodeTranslator(5351, nncli);

  auto namenodeServer = nn_translator->getRPCServer();

  std::string expectedVagrant ("vagrant");
  std::string expectedTravis ("travis");
  ASSERT_TRUE((expectedVagrant.compare(namenodeServer.getUsername()) == 0) ||
              (expectedTravis.compare(namenodeServer.getUsername()) == 0)
             );
}

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