add_library(rpcserver source/rpcserver.cc source/socket_reads.cc source/socket_writes.cc)
include_directories(${CMAKE_SOURCE_DIR}/utility/logging ${CMAKE_SOURCE_DIR}/rpcserver/include)
target_link_libraries( rpcserver easylogging++ )
