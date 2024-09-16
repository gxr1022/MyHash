# Minimum required CMake version
cmake_minimum_required(VERSION 3.10)

project(MYHASH LANGUAGES CXX)

set(CMAKE_INSTALL_INCLUDEDIR "include")

include_directories(
    /mnt/nvme0/home/gxr/Myhash/${CMAKE_INSTALL_INCLUDEDIR}
    ${GFLAGS_INCLUDE_DIRS}
)

find_package(Boost REQUIRED)
find_package(gflags REQUIRED)

add_executable(myhash src/client.cpp src/mm.cpp src/Myhash.cpp
 util/util.hpp util/checksum.hpp util/bitmap.hpp)

target_link_libraries(myhash
PRIVATE ${GFLAGS_LIBRARIES}
)