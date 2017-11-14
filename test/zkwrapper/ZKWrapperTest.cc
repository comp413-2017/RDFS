// Copyright 2017 Rice University, COMP 413 2017

#include <gtest/gtest.h>
#include <easylogging++.h>
#include <cstring>
#include <chrono>
#include <thread>
#include "zkwrapper.h"

INITIALIZE_EASYLOGGINGPP

#define LOG_CONFIG_FILE "/home/vagrant/rdfs/config/test-log-conf.conf"

namespace {

class ZKWrapperTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right
    // before each test).
    int error_code = 0;
    zk = new ZKWrapper("localhost:2181", error_code, "/testing");
    ASSERT_EQ("ZOK", zk->translate_error(error_code));  // Z_OK
  }

  static void test_watcher(
      zhandle_t *zzh,
      int type,
      int state,
      const char *path,
      void *watcherCtx
  ) {
    bool *val = reinterpret_cast<bool *>(watcherCtx);
    if (!*val) {
      *val = true;

    } else {
      printf("context is not false when in callback function\n");
    }
  }
  // Objects declared here can be used by all tests in the test case for Foo.
  ZKWrapper *zk;
};

TEST_F(ZKWrapperTest, create_sequential) {
  std::string new_path;
  int error = 0;
  bool result = zk->create_sequential(
      "/sequential-",
      ZKWrapper::EMPTY_VECTOR,
      new_path,
      false,
      error);
  ASSERT_EQ(true, result);
  ASSERT_EQ("ZOK", zk->translate_error(error));
  std::string expected("/sequential-0000000000");
  ASSERT_EQ(expected, new_path);

  std::string new_path2;
  result = zk->create_sequential(
      "/sequential-",
      ZKWrapper::EMPTY_VECTOR,
      new_path2,
      false,
      error);

  ASSERT_EQ(true, result);
  ASSERT_EQ("ZOK", zk->translate_error(error));
  std::string expected2("/sequential-0000000001");
  ASSERT_EQ(expected2, new_path2);
}

TEST_F(ZKWrapperTest, recursive_create) {
  int error = 0;
  bool result = zk->recursive_create(
      "/testrecur/test1",
      ZKWrapper::EMPTY_VECTOR, error);
  ASSERT_EQ(true, result);
  ASSERT_EQ("ZOK", zk->translate_error(error));

  /* create same path, should fail */
  result = zk->recursive_create(
      "/testrecur/test1",
      ZKWrapper::EMPTY_VECTOR, error);
  ASSERT_EQ(false, result);

  std::vector<std::uint8_t> retrieved_data(65536);
  auto data = ZKWrapper::get_byte_vector("hello");
  result = zk->recursive_create("/testrecur/test2", data, error);
  ASSERT_EQ(true, result);
  result = zk->get("/testrecur/test2", retrieved_data, error);
  ASSERT_EQ(true, result);
  ASSERT_EQ(5, retrieved_data.size());
  result = zk->get("/testrecur", retrieved_data, error);
  ASSERT_EQ(true, result);
  ASSERT_EQ(0, retrieved_data.size());

  result = zk->recursive_create(
      "/testrecur/test2/test3/test4",
      ZKWrapper::EMPTY_VECTOR, error);
  ASSERT_EQ(true, result);
  ASSERT_EQ("ZOK", zk->translate_error(error));
}

TEST_F(ZKWrapperTest, exists) {
  int error = 0;
  bool exist = false;

  bool result = zk->create("/testcreate", ZKWrapper::EMPTY_VECTOR,
                           error, false);
  ASSERT_EQ(true, result);
  ASSERT_EQ("ZOK", zk->translate_error(error));

  result = zk->exists("/testcreate", exist, error);
  ASSERT_EQ(true, result);
  ASSERT_EQ(true, exist);
  ASSERT_EQ("ZOK", zk->translate_error(error));

  result = zk->exists("/not_exists", exist, error);
  ASSERT_EQ(true, result);
  ASSERT_EQ(false, exist);
  ASSERT_EQ("ZNONODE", zk->translate_error(error));
}

TEST_F(ZKWrapperTest, wexists) {
  int error = 0;
  bool exist = false;
  bool check = false;

  bool result = zk->wexists("/testwexists", exist, test_watcher, &check, error);
  ASSERT_EQ(true, result);
  ASSERT_EQ(false, exist);
  ASSERT_EQ("ZNONODE", zk->translate_error(error));

  result = zk->create("/testwexists", ZKWrapper::EMPTY_VECTOR, error, false);
  ASSERT_EQ(true, result);
  ASSERT_EQ("ZOK", zk->translate_error(error));

  int count = 0;

  while (check == false && count < 5) {
    count++;
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }

  ASSERT_EQ(true, check);
}

TEST_F(ZKWrapperTest, get_children) {
  int error = 0;
  std::vector<std::string> children;
  bool result = zk->get_children("/", children, error);
  ASSERT_EQ(true, result);
  ASSERT_EQ("ZOK", zk->translate_error(error));
}

TEST_F(ZKWrapperTest, wget_children) {
  int error = 0;
  bool result = zk->create(
      "/testwgetchildren1",
      ZKWrapper::EMPTY_VECTOR,
      error, false);
  ASSERT_EQ(true, result);
  ASSERT_EQ("ZOK", zk->translate_error(error));

  std::vector<std::string> children;
  bool check = false;
  result = zk->wget_children(
      "/testwgetchildren1",
      children,
      test_watcher,
      &check,
      error);
  ASSERT_EQ(true, result);
  ASSERT_EQ("ZOK", zk->translate_error(error));

  result = zk->create(
      "/testwgetchildren1/test",
      ZKWrapper::EMPTY_VECTOR,
      error, false);
  ASSERT_EQ(true, result);
  ASSERT_EQ("ZOK", zk->translate_error(error));

  int count = 0;

  while (check == false && count < 5) {
    count++;
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }

  ASSERT_EQ(true, check);
}
TEST_F(ZKWrapperTest, get) {
  int error = 0;
  bool result = zk->create("/testget", ZKWrapper::EMPTY_VECTOR, error, false);
  ASSERT_EQ(true, result);
  ASSERT_EQ("ZOK", zk->translate_error(error));

  std::vector<std::uint8_t> data(65536);
  result = zk->get("/testget", data, error);
  ASSERT_EQ(true, result);
  ASSERT_EQ("ZOK", zk->translate_error(error));
  ASSERT_EQ(0, data.size());

  auto data_1 = ZKWrapper::get_byte_vector("hello");

  result = zk->create("/testget_withdata", data_1, error, false);
  ASSERT_EQ(true, result);
  ASSERT_EQ("ZOK", zk->translate_error(error));

  std::vector<std::uint8_t> retrieved_data(65536);
  result = zk->get("/testget_withdata", retrieved_data, error);
  ASSERT_EQ(true, result);
  ASSERT_EQ("ZOK", zk->translate_error(error));
  ASSERT_EQ(5, retrieved_data.size());
}

TEST_F(ZKWrapperTest, wget) {
  int error = 0;
  bool result = zk->create("/testwget1", ZKWrapper::EMPTY_VECTOR, error, false);
  ASSERT_EQ(true, result);
  ASSERT_EQ("ZOK", zk->translate_error(error));

  std::vector<std::uint8_t> data(65536);
  bool check = false;
  result = zk->wget("/testwget1", data, test_watcher, &check, error);
  ASSERT_EQ(true, result);
  ASSERT_EQ("ZOK", zk->translate_error(error));
  ASSERT_EQ(0, data.size());

  auto data1 = ZKWrapper::get_byte_vector("hello");
  result = zk->set("/testwget1", data1, error);
  ASSERT_EQ(true, result);
  ASSERT_EQ("ZOK", zk->translate_error(error));

  int count = 0;

  while (check == false && count < 5) {
    printf("waiting for check to be true\n");
    count++;
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }

  ASSERT_EQ(true, check);
}
TEST_F(ZKWrapperTest, set) {
  int error = 0;
  auto data = ZKWrapper::get_byte_vector("hello");

  bool result = zk->create("/testget1", ZKWrapper::EMPTY_VECTOR, error, false);
  ASSERT_EQ(true, result);
  ASSERT_EQ("ZOK", zk->translate_error(error));

  result = zk->set("/testget1", data, error);
  ASSERT_EQ(true, result);
  ASSERT_EQ("ZOK", zk->translate_error(error));

  std::vector<std::uint8_t> data_get(65536);
  result = zk->get("/testget1", data_get, error);
  ASSERT_EQ(true, result);
  ASSERT_EQ("ZOK", zk->translate_error(error));
  ASSERT_EQ(5, data_get.size());
}

TEST_F(ZKWrapperTest, delete_node) {
  int error = 0;

  bool result = zk->create("/testcreate2", ZKWrapper::EMPTY_VECTOR,
                           error, false);
  ASSERT_EQ(true, result);
  ASSERT_EQ("ZOK", zk->translate_error(error));

  result = zk->delete_node("/testcreate2", error);
  ASSERT_EQ(true, result);
  ASSERT_EQ("ZOK", zk->translate_error(error));

  result = zk->delete_node("/not_exists2", error);
  ASSERT_EQ(false, result);
  ASSERT_EQ("ZNONODE", zk->translate_error(error));
}

TEST_F(ZKWrapperTest, RecursiveDelete) {
  int error_code;

  zk->create("/child1", ZKWrapper::EMPTY_VECTOR, error_code, false);
  zk->create("/child2", ZKWrapper::EMPTY_VECTOR, error_code, false);
  zk->create("/child1/child2", ZKWrapper::EMPTY_VECTOR, error_code, false);
  zk->create("/child1/child3", ZKWrapper::EMPTY_VECTOR, error_code, false);

  ASSERT_TRUE(zk->recursive_delete("/child1", error_code));

  bool exists;
  ASSERT_TRUE(zk->exists("/child1", exists, error_code));
  ASSERT_EQ(false, exists);

  ASSERT_TRUE(zk->exists("/child2", exists, error_code));
  ASSERT_EQ(true, exists);
}

TEST_F(ZKWrapperTest, MultiOperation) {
  int error_code;

  auto hello_vec = ZKWrapper::get_byte_vector("hello");
  auto jello_vec = ZKWrapper::get_byte_vector("jello");
  auto bye_vec = ZKWrapper::get_byte_vector("bye");
  auto op = zk->build_create_op("/child11", hello_vec);
  auto op2 = zk->build_create_op("/child21", jello_vec);
  auto op3 = zk->build_create_op("/toDelete1", bye_vec);

  auto operations = std::vector<std::shared_ptr<ZooOp>>();

  operations.push_back(op);
  operations.push_back(op2);
  operations.push_back(op3);

  std::vector<zoo_op_result> results = std::vector<zoo_op_result>();
  ASSERT_TRUE(zk->execute_multi(operations, results, error_code));

  std::vector<std::uint8_t> vec = std::vector<std::uint8_t>();

  zk->get("/child11", vec, error_code);
  ASSERT_EQ(hello_vec, vec);
  zk->get("/child21", vec, error_code);
  ASSERT_EQ(jello_vec, vec);
  zk->get("/toDelete1", vec, error_code);
  ASSERT_EQ(bye_vec, vec);

  auto nhello_vec = ZKWrapper::get_byte_vector("new_hello");
  auto njello_vec = ZKWrapper::get_byte_vector("new_jello");
  auto op4 = zk->build_set_op("/child11", nhello_vec);
  auto op5 = zk->build_set_op("/child21", njello_vec);
  auto op6 = zk->build_delete_op("/toDelete1");

  operations = std::vector<std::shared_ptr<ZooOp>>();

  operations.push_back(op4);
  operations.push_back(op5);
  operations.push_back(op6);

  ASSERT_TRUE(zk->execute_multi(operations, results, error_code));

  zk->get("/child11", vec, error_code);
  ASSERT_EQ(nhello_vec, vec);
  zk->get("/child21", vec, error_code);
  ASSERT_EQ(njello_vec, vec);

  bool exists;
  assert(zk->exists("/toDelete1", exists, error_code));
  ASSERT_TRUE(!exists);
}
}  // namespace

int main(int argc, char **argv) {
  el::Configurations conf(LOG_CONFIG_FILE);
  el::Loggers::reconfigureAllLoggers(conf);
  el::Loggers::addFlag(el::LoggingFlag::ColoredTerminalOutput);

  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh start");
  sleep(10);
  system("sudo /home/vagrant/zookeeper/bin/zkCli.sh rmr /testing");
  ::testing::InitGoogleTest(&argc, argv);
  auto ret = RUN_ALL_TESTS();
  system("sudo /home/vagrant/zookeeper/bin/zkCli.sh rmr /testing");
  system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
  return ret;
}
