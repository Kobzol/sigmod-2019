cmake_minimum_required(VERSION 3.4)
project(sigmod2019)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -fopenmp -march=native -fno-rtti -Wall")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -flto")

OPTION(REAL_RUN "Real run" OFF)
if(REAL_RUN)
    ADD_DEFINITIONS(-DREAL_RUN)
endif()

set(SOURCE_FILES
        src/lib/util.cpp
        src/lib/io/io.cpp
        src/lib/sort/external.cpp
        src/lib/sort/inmemory.cpp
        src/lib/sort/merge.cpp
        src/lib/sort/radix.cpp
        src/lib/sort/sort.cpp
        src/lib/io/worker.cpp
)

add_library(sortlib STATIC ${SOURCE_FILES})

add_executable(sort src/main.cpp)
target_link_libraries(sort sortlib)
target_include_directories(sort PRIVATE src/lib)
