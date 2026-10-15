cmake_minimum_required(VERSION 3.25)
project(trabalho C)

set(CMAKE_C_STANDARD 23)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")

add_executable(trabalho
        feed.c
        manager.c
        manager_aux.c
        shared.c
)