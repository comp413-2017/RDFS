add_executable(zktest test.cpp)
TARGET_LINK_LIBRARIES(zktest /usr/local/lib/libzookeeper_mt.so)
