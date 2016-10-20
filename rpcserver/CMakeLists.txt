add_library(rpcserver rpcserver.cc socket_reads.cc)
include_directories(${CMAKE_SOURCE_DIR}/utility/logging)
target_link_libraries( rpcserver easylogging++ )
