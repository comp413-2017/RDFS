add_library(storage source/StorageMetrics.cc)
include_directories(${CMAKE_SOURCE_DIR}/utility/logging ${CMAKE_SOURCE_DIR}/utility/include ${CMAKE_SOURCE_DIR}/storage/include)
target_link_libraries( storage easylogging++ )

include_directories(../zkwrapper/include ../utility/include)
target_link_libraries(storage zkwrapper /usr/local/lib/libzookeeper_mt.so)