include_directories(${CMAKE_SOURCE_DIR}/zookeeper/include)

add_library(zkwrapper zkwrapper.cc)
add_library(zk_lock zk_lock.cc)
add_library(zk_queue zk_queue.cc)
target_link_libraries(zkwrapper /usr/local/lib/libzookeeper_mt.so)
target_link_libraries(zk_lock /usr/local/lib/libzookeeper_mt.so)
target_link_libraries(zk_queue /usr/local/lib/libzookeeper_mt.so)
