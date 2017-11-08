add_library(web-request source/webRequestTranslator.cc)
include_directories(${CMAKE_SOURCE_DIR}/utility/logging
        ${CMAKE_SOURCE_DIR}/web-request/include
        ${CMAKE_SOURCE_DIR}/zkwrapper/include
        ${CMAKE_SOURCE_DIR}/zookeeper/include
        ${CMAKE_SOURCE_DIR}/native-filesystem/include
        ${CMAKE_SOURCE_DIR}/utility/config-reader
        ${CMAKE_SOURCE_DIR}/utility/logging
        ${CMAKE_SOURCE_DIR}/utility/logging
        ${CMAKE_SOURCE_DIR}/utility/include
        ${CMAKE_SOURCE_DIR}/rice-datanode/include
        ${CMAKE_SOURCE_DIR}/native-filesystem/include
        ${CMAKE_SOURCE_DIR}/rpcserver/include)
target_link_libraries(web-request easylogging++)
target_link_libraries(web-request protobuf)
