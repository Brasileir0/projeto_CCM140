cmake_minimum_required(VERSION 3.15)
project(bbs_project)

set(CMAKE_CXX_STANDARD 17)

find_package(ZeroMQ REQUIRED)
find_package(cppzmq REQUIRED)
find_package(msgpack-cxx REQUIRED)

add_executable(server server.cpp)
target_linklib_libraries(server PRIVATE libzmq cppzmq msgpack-cxx)

add_executable(cliente cliente.cpp)
target_linklib_libraries(cliente PRIVATE libzmq cppzmq msgpack-cxx)