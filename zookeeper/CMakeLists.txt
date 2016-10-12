include_directories(${CMAKE_SOURCE_DIR}/zkwrapper)
include_directories(${CMAKE_SOURCE_DIR}/zookeeper/include)

add_library(zk_client_common source/zk_client_common.cc)

add_executable(zktest source/test.cpp)
target_link_libraries(zktest zkwrapper)
target_link_libraries(zktest /usr/local/lib/libzookeeper_mt.so)


add_executable(dn_client_test source/zk_dn_client_test.cc)
target_link_libraries(dn_client_test zkwrapper)
target_link_libraries(dn_client_test zk_client_common)
target_link_libraries(dn_client_test /usr/local/lib/libzookeeper_mt.so)
