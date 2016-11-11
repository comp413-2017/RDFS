include_directories(${CMAKE_SOURCE_DIR}/utility/logging ${CMAKE_SOURCE_DIR}/native-filesystem/include)
add_library(native-fs source/native_filesystem.cc)
target_link_libraries(native-fs easylogging++)
