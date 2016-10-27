cmake_minimum_required(VERSION 2.8)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Locate GTest
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

# Locate GMock
include_directories(${GMOCK_INCLUDE_DIRS})

set(GTEST "/usr/src/gtest/libgtest.a" "/usr/src/gmock/libgmock.a")

# Create an executable called runTests with the test file you want to run
add_executable(runTests tests.cc)
include_directories(/usr/local/include/zookeeper)
include_directories(../zkwrapper)
link_directories(/home/vagrant/rdfs/build/zkwrapper)

add_executable(ZKWrapperTest zkwrapper/ZKWrapperTest.cc ../zkwrapper/zkwrapper.h)
target_link_libraries(ZKWrapperTest zkwrapper /usr/local/lib/libzookeeper_mt.so)
target_link_libraries(ZKWrapperTest ${GTEST_LIBRARIES} pthread)
target_link_libraries(ZKWrapperTest ${GTEST})

add_executable(ZKLockTest zkwrapper/ZKLockTest.cc ../zkwrapper/zk_lock.h)
target_link_libraries(ZKLockTest zk_lock zkwrapper /usr/local/lib/libzookeeper_mt.so)
target_link_libraries(ZKLockTest ${GTEST_LIBRARIES} pthread)
target_link_libraries(ZKLockTest ${GTEST})

add_executable(ZKQueueTest zkwrapper/ZKQueueTest.cc ../zkwrapper/zk_queue.h)
target_link_libraries(ZKQueueTest zk_queue zkwrapper /usr/local/lib/libzookeeper_mt.so)
target_link_libraries(ZKQueueTest ${GTEST_LIBRARIES} pthread)
target_link_libraries(ZKQueueTest ${GTEST})

target_link_libraries(runTests ${GTEST_LIBRARIES} pthread)
target_link_libraries(runTests ${GTEST})
