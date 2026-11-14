cmake_minimum_required(VERSION 3.10)
project(projeto_ccm140)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(PkgConfig REQUIRED)
pkg_check_modules(libzmq REQUIRED libzmq)

find_path(MSGPACK_INCLUDE_DIR msgpack.hpp REQUIRED)

find_package(Threads REQUIRED)

add_executable(server server.cpp)
target_include_directories(server PRIVATE ${libzmq_INCLUDE_DIRS} ${MSGPACK_INCLUDE_DIR})
target_link_libraries(server PRIVATE ${libzmq_LIBRARIES})

add_executable(client client.cpp)
target_include_directories(client PRIVATE ${libzmq_INCLUDE_DIRS} ${MSGPACK_INCLUDE_DIR})
target_link_libraries(client PRIVATE ${libzmq_LIBRARIES} Threads::Threads)

add_executable(broker broker.cpp)
target_include_directories(broker PRIVATE ${libzmq_INCLUDE_DIRS})
target_link_libraries(broker PRIVATE ${libzmq_LIBRARIES})

add_executable(referencia referencia.cpp)
target_include_directories(referencia PRIVATE ${libzmq_INCLUDE_DIRS} ${MSGPACK_INCLUDE_DIR})
target_link_libraries(referencia PRIVATE ${libzmq_LIBRARIES})

add_executable(proxy_pubsub proxy_pubsub.cpp)
target_include_directories(proxy_pubsub PRIVATE ${libzmq_INCLUDE_DIRS})
target_link_libraries(proxy_pubsub PRIVATE ${libzmq_LIBRARIES})