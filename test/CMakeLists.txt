# Locate GTest
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

# Locate GMock
include_directories(${GMOCK_INCLUDE_DIRS})

set(GTEST "/usr/src/gtest/libgtest.a" "/usr/src/gmock/libgmock.a")

# Create an executable called runTests with the test file you want to run
add_executable(runTests tests.cc)
target_link_libraries(runTests ${GTEST_LIBRARIES} pthread)
target_link_libraries(runTests ${GTEST})
include_directories(../dummy-filesystem)
link_directories(/home/vagrant/rdfs/build/dummy-filesystem)
