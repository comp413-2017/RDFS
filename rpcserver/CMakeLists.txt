add_library(rpcserver rpcserver.cc socket_reads.cc socket_writes.cc)
include_directories(${CMAKE_SOURCE_DIR}/utility/logging)
target_link_libraries( rpcserver easylogging++ )
