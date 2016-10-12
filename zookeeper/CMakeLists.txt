include_directories(${CMAKE_SOURCE_DIR}/zkwrapper)
include_directories(${CMAKE_SOURCE_DIR}/zookeeper)

add_executable(zktest test.cpp)

target_link_libraries(zktest zkwrapper)
target_link_libraries(zktest /usr/local/lib/libzookeeper_mt.so)

add_executable(dn_client_test zk_dn_client_test.cc)

target_link_libraries(dn_client_test zkwrapper)
target_link_libraries(dn_client_test /usr/local/lib/libzookeeper_mt.so)
