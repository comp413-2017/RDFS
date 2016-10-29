# Locate GTest
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

# Locate GMock
include_directories(${GMOCK_INCLUDE_DIRS})

set(GTEST "/usr/src/gtest/libgtest.a" "/usr/src/gmock/libgmock.a")

# Create an executable called runTests with the test file you want to run
add_executable(NativeFsTest native-filesystem/native-fs-tests.cc)
target_link_libraries(NativeFsTest ${GTEST_LIBRARIES} pthread)
target_link_libraries(NativeFsTest ${GTEST})
target_link_libraries(NativeFsTest native-fs)
include_directories(../native-filesystem)
link_directories(/home/vagrant/rdfs/build/native-filesystem)
