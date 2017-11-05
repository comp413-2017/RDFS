include_directories(
    ${CMAKE_SOURCE_DIR}/rpcserver/include
    ${CMAKE_SOURCE_DIR}/utility/web-server
    ${CMAKE_SOURCE_DIR}/utility/logging
    ${CMAKE_SOURCE_DIR}/utility/include
    ${CMAKE_SOURCE_DIR}/web-rdfs/include
    ${CMAKE_SOURCE_DIR}/zookeeper/include
)

add_library(web_rdfs_server source/web_rdfs_server.cc)
target_link_libraries(web_rdfs_server easylogging++)
target_link_libraries(web_rdfs_server protobuf)
target_link_libraries(web_rdfs_server pthread)
target_link_libraries(web_rdfs_server ${Boost_LIBRARIES})

add_library(http_handlers source/http_handlers.cc)
target_link_libraries(http_handlers easylogging++)

add_executable(webrdfs source/main.cc)
target_link_libraries(webrdfs web_rdfs_server)
target_link_libraries(webrdfs http_handlers)
