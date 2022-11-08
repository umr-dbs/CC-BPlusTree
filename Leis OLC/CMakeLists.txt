cmake_minimum_required(VERSION 3.23)
project(Leis_OLC)

set(CMAKE_CXX_STANDARD 14)

find_library(randGen libs/librandGen.so)
add_executable(${PROJECT_NAME} main.cpp)

target_link_libraries(${PROJECT_NAME} randGen)
