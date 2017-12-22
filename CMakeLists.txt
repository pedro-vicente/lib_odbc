#CMake build, Pedro Vicente
cmake_minimum_required(VERSION 2.8)
project (lib_odbc)

#/////////////////////////////////////////////////////////////////////////////////////
#general C/C++ build flags
#/////////////////////////////////////////////////////////////////////////////////////

set(CMAKE_BUILD_TYPE Debug)
set(BUILD_STATIC_LIBS ON)
set(BUILD_SHARED_LIBS OFF)

if (MSVC)
  add_definitions( "/D_CRT_SECURE_NO_WARNINGS /D_WINSOCK_DEPRECATED_NO_WARNINGS /D_CRT_NONSTDC_NO_DEPRECATE" )
  message("-- Building with static runtime library")
  set(CMAKE_CXX_FLAGS_DEBUG "/D_DEBUG /MTd /Zi /Ob0 /Od /RTC1")
  set(CMAKE_CXX_FLAGS_MINSIZEREL     "/MT /O1 /Ob1 /D NDEBUG")
  set(CMAKE_CXX_FLAGS_RELEASE        "/MT /O2 /Ob2 /D NDEBUG")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "/MT /Zi /O2 /Ob1 /D NDEBUG")
endif(MSVC)

#//////////////////////////
# system specific libraries needed to link (assume existing)
#//////////////////////////

if(MSVC)
  set(LIB_DEP ${LIB_DEP} ws2_32.lib winmm.lib wldap32.lib odbc32.lib)
endif()

if(UNIX)
  set(LIB_DEP ${LIB_DEP} odbc)
endif()

set(sources ${sources})
set(sources ${sources} csv.hh)
set(sources ${sources} csv.cc)
set(sources ${sources} clock.hh)
set(sources ${sources} clock.cc)
set(sources ${sources} odbc.hh)
set(sources ${sources} odbc.cc)

add_executable(dc311 dc311.cc ${sources})
target_link_libraries (dc311 ${LIB_DEP})


