project(rdfs)
cmake_minimum_required(VERSION 2.8)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
find_package(Protobuf REQUIRED)
find_package(Boost REQUIRED COMPONENTS system)
include_directories(${Boost_INCLUDE_DIR})
link_directories(${Boost_LIBRARY_DIR})

include_directories(/usr/include/asio)

add_subdirectory(proto)
include_directories(${ProtobufIncludePath})

add_subdirectory(test)

add_subdirectory(utility)
add_subdirectory(rpcserver)
add_subdirectory(rice-namenode)

file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/config/hdfs-default.xml
        DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/rice-namenode/)
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/config/nn-log-conf.conf
        DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/rice-namenode/)
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/config/dn-log-conf.conf
        DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/rice-datanode/)
