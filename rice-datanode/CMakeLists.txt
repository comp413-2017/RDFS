
include_directories(${CMAKE_SOURCE_DIR}/rpcserver ${CMAKE_SOURCE_DIR}/utility/config-reader ${CMAKE_SOURCE_DIR}/utility/pugixml/src ${CMAKE_SOURCE_DIR}/utility/logging ${CMAKE_SOURCE_DIR}/zookeeper/include ${CMAKE_SOURCE_DIR}/zkwrapper)


add_executable(datanode main.cc ClientDatanodeProtocolImpl.cc data_transfer_server.cc)
target_link_libraries( datanode rpcserver )
target_link_libraries( datanode config )
target_link_libraries( datanode easylogging++ )
target_link_libraries( datanode protobuf )
target_link_libraries( datanode pthread )
target_link_libraries( datanode messages )
target_link_libraries( datanode zk_client_dn)
target_link_libraries( datanode zkwrapper)
target_link_libraries( datanode ${Boost_LIBRARIES} )
