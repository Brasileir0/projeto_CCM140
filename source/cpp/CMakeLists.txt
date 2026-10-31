cmake_minimum_required(VERSION 3.15)
project(bbs_project)

set(CMAKE_CXX_STANDARD 17)

find_package(PkgConfig REQUIRED)
pkg_check_modules(ZeroMQ REQUIRED IMPORTED_TARGET libzmq)

add_executable(server server.cpp)
target_link_libraries(server PRIVATE PkgConfig::ZeroMQ)

add_executable(client client.cpp)
target_link_libraries(client PRIVATE PkgConfig::ZeroMQ)

add_executable(broker broker.cpp)
target_link_libraries(broker PRIVATE PkgConfig::ZeroMQ)