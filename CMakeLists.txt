# CMakeList.txt : Top-level CMake project file, do global configuration
# and include sub-projects here.
#
cmake_minimum_required (VERSION 3.17)

enable_testing()

project ("Phantom.Scalable.Native")

set(CMAKE_CXX_STANDARD 20)

if (MSVC)
	add_compile_options("/await")
endif()

add_subdirectory ("Phantom.System")
add_subdirectory ("Phantom.System.Test")
add_subdirectory ("Phantom.ProtoStore")
