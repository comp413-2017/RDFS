project(rdfs)
cmake_minimum_required(VERSION 2.8)

find_package(Protobuf REQUIRED)
add_subdirectory(proto)
add_definitions(-std=c++11)
