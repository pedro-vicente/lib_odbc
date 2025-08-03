#CMake build, Pedro Vicente
cmake_minimum_required(VERSION 4.0)
set(CMAKE_BUILD_TYPE Debug)
project (lib_odbc CXX C)

set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
set(CMAKE_CXX_STANDARD 17)

if (MSVC)
  add_definitions(-D_CRT_SECURE_NO_WARNINGS)
  add_definitions(-D_CRT_NONSTDC_NO_DEPRECATE)
endif()

include_directories(sqlite-3.21.0)

set(src ${src})
set(src ${src} csv.hh)
set(src ${src} csv.cc)
set(src ${src} clock.hh)
set(src ${src} clock.cc)
set(src ${src} odbc.hh)
set(src ${src} odbc.cc)
set(src ${src} table.hh)

add_library(lib_odbc ${src})

#//////////////////////////
#link with libraries
#lib_dep contains a cascade definition of all the libraries needed to link
#//////////////////////////

set(lib_dep ${lib_dep})
set(lib_dep ${lib_dep} lib_odbc)

#//////////////////////////
# system specific libraries needed to link 
#//////////////////////////

#//////////////////////////
# ODBC
#//////////////////////////

set(ODBC_LIBRARY ${ODBC_LIBRARY})
if (MSVC)
  set(ODBC_LIBRARY ${ODBC_LIBRARY} odbc32.lib odbccp32.lib)
elseif(UNIX)
  find_path(ODBC_INCLUDE sql.h ${find_opt})
  if(NOT ODBC_INCLUDE)
    message(FATAL_ERROR "sql.h header file not found")
  else()
    message("-- Found sql.h header file at: " ${ODBC_INCLUDE})
  endif()
  include_directories(${ODBC_INCLUDE})
  find_library(ODBC_LIBRARY NAMES odbc PATHS "/usr/local/lib")
  if(NOT ODBC_LIBRARY)
    message(FATAL_ERROR "ODBC library not found$")
  else()
    message("-- Found ODBC library at: " ${ODBC_LIBRARY})
  endif()
endif()

set(lib_dep ${lib_dep} ${ODBC_LIBRARY})


add_executable(insert insert.cc)
add_executable(csv_odbc csv_odbc.cc)
add_executable(dc311 dc311.cc sqlite-3.21.0/sqlite3.c sqlite-3.21.0/sqlite3.h)

target_link_libraries (insert ${lib_dep})
target_link_libraries (dc311 ${lib_dep})
target_link_libraries (csv_odbc ${lib_dep})

set_property(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY VS_STARTUP_PROJECT dc311)

#//////////////////////////
# copy DC311 CSV input to the build directory
#//////////////////////////

file(COPY "${CMAKE_SOURCE_DIR}/dc_311-2016_100.csv" DESTINATION ${CMAKE_BINARY_DIR})

set_target_properties(dc311 PROPERTIES
    VS_DEBUGGER_COMMAND_ARGUMENTS "-S localhost -d dc311 -f dc_311-2016_100.csv"
    VS_DEBUGGER_WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
)





