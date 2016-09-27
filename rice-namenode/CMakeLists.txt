include_directories(${CMAKE_SOURCE_DIR}/rpcserver)

add_executable(namenode main.cc)
target_link_libraries( namenode rpcserver )
target_link_libraries( namenode protobuf )
target_link_libraries( namenode pthread )
target_link_libraries( namenode messages )
target_link_libraries( namenode ${Boost_LIBRARIES} )
