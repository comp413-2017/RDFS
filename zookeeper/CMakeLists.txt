include_directories(${CMAKE_SOURCE_DIR}/zkwrapper)
include_directories(${CMAKE_SOURCE_DIR}/zookeeper/include)

add_library(zk_client_common source/zk_client_common.cc)
target_link_libraries(zk_client_common zkwrapper)

add_library(zk_client_dn source/zk_client_dn.cc)
target_link_libraries(zk_client_dn zk_client_common)

add_library(zk_client_nn source/zk_nn_client.cc)
target_link_libraries(zk_client_nn zk_client_common)

add_executable(zktest source/test.cpp)
target_link_libraries(zktest zkwrapper)
target_link_libraries(zktest /usr/local/lib/libzookeeper_mt.so)

add_executable(dn_client_test source/zk_dn_client_test.cc)
target_link_libraries(dn_client_test zkwrapper)
target_link_libraries(dn_client_test zk_client_dn)
target_link_libraries(dn_client_test /usr/local/lib/libzookeeper_mt.so)

add_executable(nn_client_test source/zk_nn_client_test.cc)
target_link_libraries(nn_client_test zkwrapper)
target_link_libraries(nn_client_test zk_client_nn)
target_link_libraries(nn_client_test /usr/local/lib/libzookeeper_mt.so)