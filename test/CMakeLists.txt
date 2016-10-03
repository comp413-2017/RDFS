# Locate GTest
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

# Create an executable called runTests with the test file you want to run
add_executable(runTests tests.cc)
target_link_libraries(runTests ${GTEST_LIBRARIES} pthread)
