//
// Created by Nicholas Kwon on 10/16/16.
//

#include <gtest/gtest.h>
#include <stdlib.h>
#include <future>
#include <chrono>
#include "zk_lock.h"

#include <easylogging++.h>

INITIALIZE_EASYLOGGINGPP

namespace {
	const std::string zk_dir = "/a";
	class ZKLockTest : public ::testing::Test {
	protected:
		virtual void SetUp() {
			// Code here will be called immediately after the constructor (right
			// before each test).
			int errorCode;
			zkWrapper = new ZKWrapper("localhost:2181", errorCode);
			assert(errorCode == 0); // ZOK
		}

		static void lock_and_write(ZKWrapper &zkWrapper, std::condition_variable &cv, int &x) {
			ZKLock lock1(zkWrapper, zk_dir);
			ASSERT_EQ(0, lock1.lock());
			cv.notify_one();
			x = 5;
			std::this_thread::sleep_for(std::chrono::seconds(2));
			ASSERT_EQ(0, lock1.unlock());
		}

		static void lock_and_read(ZKWrapper &zkWrapper,std::condition_variable &cv, int &x, std::promise<int> &p) {
			ZKLock lock2(zkWrapper, zk_dir);
			std::mutex mtx;
			std::unique_lock<std::mutex> lck(mtx);
			cv.wait(lck);
			ASSERT_EQ(0, lock2.lock());
			p.set_value(x);
			ASSERT_EQ(0, lock2.unlock());
		}

		// Objects declared here can be used by all tests in the test case for Foo.
		ZKWrapper *zkWrapper;
	};



	TEST_F(ZKLockTest, AtomicInteger) {
		std::condition_variable cv;
		std::promise<int> promise;
		auto future = promise.get_future();
		int x = 0;
		std::thread thread1(lock_and_write, std::ref(*zkWrapper), std::ref(cv), std::ref(x));
		std::thread thread2(lock_and_read, std::ref(*zkWrapper), std::ref(cv), std::ref(x), std::ref(promise));
		thread1.join();
		thread2.join();

		ASSERT_EQ(5, future.get());
	}
}

int main(int argc, char **argv) {
	system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");
	system("sudo /home/vagrant/zookeeper/bin/zkServer.sh start");
	sleep(10);

	::testing::InitGoogleTest(&argc, argv);
	auto ret = RUN_ALL_TESTS();

	system("sudo /home/vagrant/zookeeper/bin/zkCli.sh rmr /_locknode_");
	system("sudo /home/vagrant/zookeeper/bin/zkServer.sh stop");

	return ret;
}
