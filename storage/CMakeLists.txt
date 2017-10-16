add_library(storage source/StorageMetrics.cc)
include_directories(${CMAKE_SOURCE_DIR}/utility/logging ${CMAKE_SOURCE_DIR}/storage/include)
target_link_libraries( storage easylogging++ )

# Locate GTest
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

set(GTEST "/usr/src/gtest/libgtest.a" "/usr/src/gmock/libgmock.a")
target_link_libraries(storage ${GTEST_LIBRARIES} pthread)
target_link_libraries(storage ${GTEST})

include_directories(../zkwrapper/include)
target_link_libraries(storage zkwrapper /usr/local/lib/libzookeeper_mt.so)