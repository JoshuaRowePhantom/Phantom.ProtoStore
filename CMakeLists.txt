cmake_minimum_required (VERSION 3.24)

find_package(cmakerc REQUIRED)
find_package(gtest REQUIRED)
find_package(flatbuffers CONFIG REQUIRED )

enable_testing()

project ("Phantom.Scalable.ProtoStore")
set(CMAKE_COMPILE_WARNING_AS_ERROR ON)
add_compile_options($<$<CXX_COMPILER_ID:MSVC>:/W4>)

set(CMAKE_CXX_STANDARD 23)
if (MSVC)
    add_compile_definitions(_SILENCE_ALL_CXX20_DEPRECATION_WARNINGS)
    add_compile_definitions(_SILENCE_ALL_CXX17_DEPRECATION_WARNINGS)
endif()

include(cmake/FlatBuffersSchemaResource.cmake)

add_subdirectory ("Phantom.ProtoStore")
add_subdirectory ("Phantom.ProtoStore.Test")
add_subdirectory ("Phantom.ProtoStore.Utility")
