include_directories(${CMAKE_SOURCE_DIR}/isal/include)

add_library(isal source/dump.c source/erasure_code.c source/erasure_coder.c source/gf_util.c source/isal_load.c)