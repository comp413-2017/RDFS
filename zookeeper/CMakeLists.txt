include_directories(${CMAKE_SOURCE_DIR}/zkwrapper)

add_executable(zktest test.cpp)

target_link_libraries(zktest zkwrapper)
target_link_libraries(zktest /usr/local/lib/libzookeeper_mt.so)
