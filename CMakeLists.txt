cmake_minimum_required (VERSION 3.24)

enable_testing()

project ("Phantom.Scalable.Native")

if (MSVC)
	add_compile_options("/std:c++latest")
	add_compile_options("/WX")

	add_compile_definitions(_SILENCE_ALL_CXX20_DEPRECATION_WARNINGS)
	add_compile_definitions(_SILENCE_ALL_CXX17_DEPRECATION_WARNINGS)
endif()

add_subdirectory ("Phantom.System")
add_subdirectory ("Phantom.System.Test") 
add_subdirectory ("Phantom.ProtoStore")
add_subdirectory ("Phantom.ProtoStore.Test")
add_subdirectory ("Phantom.ProtoStore.Utility")
#add_subdirectory ("Phantom.Scalable")
#add_subdirectory ("Phantom.Scalable.Test")
