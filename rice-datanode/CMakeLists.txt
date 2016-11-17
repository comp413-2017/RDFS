include_directories(${CMAKE_SOURCE_DIR}/rpcserver/include
		${CMAKE_SOURCE_DIR}/utility/config-reader
		${CMAKE_SOURCE_DIR}/utility/logging
		${CMAKE_SOURCE_DIR}/utility/include
		${CMAKE_SOURCE_DIR}/zookeeper/include
		${CMAKE_SOURCE_DIR}/zkwrapper/include
		${CMAKE_SOURCE_DIR}/native-filesystem/include
		${CMAKE_SOURCE_DIR}/rice-datanode/include)

add_library(data_transfer_server source/data_transfer_server.cc)
add_executable(datanode source/main.cc source/ClientDatanodeProtocolImpl.cc)

target_link_libraries( datanode rpcserver )
target_link_libraries( datanode config )

target_link_libraries( datanode data_transfer_server )
target_link_libraries( datanode easylogging++ )
target_link_libraries( datanode native-fs )
target_link_libraries( datanode protobuf )
target_link_libraries( datanode pthread )
target_link_libraries( datanode messages )
target_link_libraries( datanode zk_client_dn )
target_link_libraries( datanode zkwrapper)
target_link_libraries( datanode ${Boost_LIBRARIES} )
