include_directories(${CMAKE_SOURCE_DIR}/zookeeper/include ${CMAKE_SOURCE_DIR}/utility/logging)
include_directories(${CMAKE_SOURCE_DIR}/zkwrapper/include)

add_library(zkwrapper source/zkwrapper.cc)
add_library(zk_lock source/zk_lock.cc)
target_link_libraries(zkwrapper /usr/local/lib/libzookeeper_mt.so)
target_link_libraries(zkwrapper easylogging++ )
target_link_libraries(zk_lock easylogging++ )
target_link_libraries(zk_lock /usr/local/lib/libzookeeper_mt.so)
