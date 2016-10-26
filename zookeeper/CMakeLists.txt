include_directories(${CMAKE_SOURCE_DIR}/zkwrapper)
include_directories(${CMAKE_SOURCE_DIR}/zookeeper/include)
include_directories(${CMAKE_SOURCE_DIR}/utility/config-reader)
include_directories(${CMAKE_SOURCE_DIR}/utility/logging)

add_library(zk_client_common source/zk_client_common.cc)
target_link_libraries(zk_client_common zkwrapper)
target_link_libraries(zk_client_common easylogging++)

add_library(zk_client_dn source/zk_client_dn.cc)
target_link_libraries(zk_client_dn zk_client_common)
target_link_libraries(zk_client_dn easylogging++)

add_library(zk_client_nn source/zk_nn_client.cc)
target_link_libraries(zk_client_nn zk_client_common)
target_link_libraries(zk_client_nn easylogging++)

