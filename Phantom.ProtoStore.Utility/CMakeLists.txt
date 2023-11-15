# CMakeList.txt : Top-level CMake project file, do global configuration
# and include sub-projects here.
#
cmake_minimum_required (VERSION 3.24)

add_executable(
    Phantom.ProtoStore.Utility
    "main.cpp" 
    "DumpHeader.cpp"
    "DumpLog.cpp"
    "DumpPartition.cpp"
    "Utility.h"
    )

target_compile_definitions(
    Phantom.ProtoStore.Utility
    PUBLIC
    "BOOST_ALL_NO_LIB=1"
)

target_link_libraries(
    Phantom.ProtoStore.Utility
    PRIVATE
    Phantom.System
    Phantom.ProtoStore.Internal
)
