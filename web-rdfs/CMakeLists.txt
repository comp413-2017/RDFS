include_directories(
    ${CMAKE_SOURCE_DIR}/rpcserver/include
    ${CMAKE_SOURCE_DIR}/utility/web-server
    ${CMAKE_SOURCE_DIR}/utility/logging
    ${CMAKE_SOURCE_DIR}/utility/include
    ${CMAKE_SOURCE_DIR}/web-rdfs/include
    ${CMAKE_SOURCE_DIR}/zookeeper/include
)

add_library(rdfs_server source/rdfs_server.cc)
target_link_libraries(rdfs_server rpcserver)
target_link_libraries(rdfs_server easylogging++)
target_link_libraries(rdfs_server protobuf)
target_link_libraries(rdfs_server pthread)
target_link_libraries(rdfs_server zkwrapper)
target_link_libraries(rdfs_server ${Boost_LIBRARIES})

add_executable(webrdfs source/main.cc)
target_link_libraries(webrdfs rdfs_server)
