include_directories(${CMAKE_SOURCE_DIR}/zkwrapper/include
        ${CMAKE_SOURCE_DIR}/zookeeper/include
		${CMAKE_SOURCE_DIR}/native-filesystem/include
        ${CMAKE_SOURCE_DIR}/utility/config-reader
        ${CMAKE_SOURCE_DIR}/utility/logging
        ${CMAKE_SOURCE_DIR}/utility/logging
        ${CMAKE_SOURCE_DIR}/utility/include
        ${CMAKE_SOURCE_DIR}/rice-datanode/include
        ${CMAKE_SOURCE_DIR}/native-filesystem/include
        ${CMAKE_SOURCE_DIR}/rpcserver/include
		${CMAKE_SOURCE_DIR}/isal/include)

add_library(zk_client_common source/zk_client_common.cc)
target_link_libraries(zk_client_common zkwrapper)
target_link_libraries(zk_client_common easylogging++)
target_link_libraries(zk_client_common isal)
target_link_libraries(zk_client_common /usr/lib/x86_64-linux-gnu/libdl.so)

add_library(zk_client_dn source/zk_dn_client.cc)
target_link_libraries(zk_client_dn zk_client_common)
target_link_libraries(zk_client_dn easylogging++)
target_link_libraries(zk_client_dn protobuf)
target_link_libraries(zk_client_dn zk_lock)
target_link_libraries(zk_client_dn data_transfer_server)

add_library(zk_client_nn source/zk_nn_client.cc)
target_link_libraries(zk_client_nn zk_client_common)
target_link_libraries(zk_client_nn easylogging++)
target_link_libraries(zk_client_nn protobuf)
target_link_libraries(zk_client_nn config)
target_link_libraries(zk_client_nn zk_lock)
