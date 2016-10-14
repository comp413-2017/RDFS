include_directories(${CMAKE_SOURCE_DIR}/utility/pugixml/src ${CMAKE_SOURCE_DIR}/utility/logging)

add_library(config ConfigReader.cc)

target_link_libraries( config pugixml )
target_link_libraries( config easylogging++ )
