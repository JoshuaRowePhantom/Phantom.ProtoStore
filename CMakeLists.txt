# CMakeList.txt : Top-level CMake project file, do global configuration
# and include sub-projects here.
#
cmake_minimum_required (VERSION 3.17)

enable_testing()

project ("Phantom.Scalable.Native")

set(CMAKE_CXX_STANDARD 20)

if (MSVC)
	add_compile_options("/std:c++latest")
	add_compile_options("/WX")

	add_compile_options("$<$<CONFIG:RELWITHDEBINFO>:/GL>")
	add_link_options("$<$<CONFIG:RELWITHDEBINFO>:/LTCG>")
	add_compile_definitions(_SILENCE_ALL_CXX20_DEPRECATION_WARNINGS)
	add_compile_definitions(_SILENCE_ALL_CXX17_DEPRECATION_WARNINGS)
endif()

include("googletest")
include("cmake/PhantomProtobuf.cmake")

add_subdirectory ("Phantom.System")
add_subdirectory ("Phantom.System.Test") 
add_subdirectory ("Phantom.ProtoStore")
add_subdirectory ("Phantom.ProtoStore.Test")
add_subdirectory ("Phantom.ProtoStore.Utility")
