add_library(zkwrapper zkwrapper.cc)
target_link_libraries(zkwrapper /usr/local/lib/libzookeeper_mt.so)
