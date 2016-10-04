add_library(rpcserver rpcserver.cc)
include_directories(${CMAKE_SOURCE_DIR}/utility/logging)
target_link_libraries( rpcserver easylogging++ )