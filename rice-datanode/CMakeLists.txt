include_directories(${CMAKE_SOURCE_DIR}/rpcserver ${CMAKE_SOURCE_DIR}/utility/pugixml/src)

add_executable(datanode main.cc ClientDatanodeProtocolImpl.cc)
target_link_libraries( datanode rpcserver )
target_link_libraries( datanode pugixml )
target_link_libraries( datanode protobuf )
target_link_libraries( datanode pthread )
target_link_libraries( datanode messages )
target_link_libraries( datanode ${Boost_LIBRARIES} )