include_directories(${CMAKE_SOURCE_DIR}/isal/include)

add_library(isal source/isal_load.c source/erasure_code.c source/gf_util.c source/erasure_coder.c source/dump.c)
target_link_libraries(isal /usr/lib/libisal.so)
target_link_libraries(isal /usr/lib/x86_64-linux-gnu/libdl.so)
target_link_libraries(isal easylogging++)
