include_directories(${CMAKE_SOURCE_DIR}/rpcserver/include
        ${CMAKE_SOURCE_DIR}/utility/config-reader
        ${CMAKE_SOURCE_DIR}/utility/logging
        ${CMAKE_SOURCE_DIR}/zkwrapper/include
        ${CMAKE_SOURCE_DIR}/zookeeper/include
        ${CMAKE_SOURCE_DIR}/rice-namenode/include
        ${CMAKE_SOURCE_DIR}/utility/include)

add_library(namenode_protos source/ClientNamenodeProtocolImpl.cc)
target_link_libraries( namenode_protos rpcserver )
target_link_libraries( namenode_protos zkwrapper )
target_link_libraries( namenode_protos rpcserver )
target_link_libraries( namenode_protos config )
target_link_libraries( namenode_protos easylogging++ )
target_link_libraries( namenode_protos protobuf )
target_link_libraries( namenode_protos pthread )
target_link_libraries( namenode_protos messages )
target_link_libraries( namenode_protos zk_client_dn )
target_link_libraries( namenode_protos zk_client_nn )
target_link_libraries( namenode_protos /usr/local/lib/libzookeeper_mt.so )
target_link_libraries( namenode_protos ${Boost_LIBRARIES} )

add_executable(namenode source/main.cc )

target_link_libraries(namenode namenode_protos)

add_library(ha_protos source/HaServiceProtocolImpl.cc )
target_link_libraries( ha_protos rpcserver )
target_link_libraries( ha_protos zkwrapper )
target_link_libraries( ha_protos rpcserver )
target_link_libraries( ha_protos config )
target_link_libraries( ha_protos easylogging++ )
target_link_libraries( ha_protos protobuf )
target_link_libraries( ha_protos pthread )
target_link_libraries( ha_protos messages )
target_link_libraries( ha_protos zk_client_dn )
target_link_libraries( ha_protos zk_client_nn )
target_link_libraries( ha_protos /usr/local/lib/libzookeeper_mt.so )
target_link_libraries( ha_protos ${Boost_LIBRARIES} )

target_link_libraries(namenode ha_protos)
