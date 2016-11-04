include_directories(${CMAKE_SOURCE_DIR}/utility/logging)
add_library(native-fs native-filesystem.cc)
target_link_libraries(native-fs easylogging++)
