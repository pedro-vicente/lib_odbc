#CMake build, Pedro Vicente
cmake_minimum_required(VERSION 3.19)
set(CMAKE_BUILD_TYPE Debug)
project (db CXX C)

set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
set(CMAKE_CXX_STANDARD 17)

if (MSVC)
  add_definitions(-D_CRT_SECURE_NO_WARNINGS)
  add_definitions(-D_CRT_NONSTDC_NO_DEPRECATE)
endif()

include_directories(sqlite-3.21.0)

set(sources ${sources})
set(sources ${sources} csv.hh)
set(sources ${sources} csv.cc)
set(sources ${sources} clock.hh)
set(sources ${sources} clock.cc)
set(sources ${sources} odbc.hh)
set(sources ${sources} odbc.cc)

add_library(lib_odbc ${sources})

#//////////////////////////
#link with libraries
#lib_dep contains a cascade definition of all the libraries needed to link
#//////////////////////////

set(lib_dep ${lib_dep})
set(lib_dep ${lib_dep} lib_odbc)

#//////////////////////////
# system specific libraries needed to link (assume existing)
#//////////////////////////

if(MSVC)
  set(lib_dep ${lib_dep} odbc32.lib)
elseif(UNIX)
  set(lib_dep ${lib_dep} odbc)
endif()

add_executable(insert insert.cc)
add_executable(csv_odbc csv_odbc.cc)
add_executable(dc311 dc311.cc sqlite-3.21.0/sqlite3.c sqlite-3.21.0/sqlite3.h)

target_link_libraries (insert ${lib_dep})
target_link_libraries (dc311 ${lib_dep})
target_link_libraries (csv_odbc ${lib_dep})



