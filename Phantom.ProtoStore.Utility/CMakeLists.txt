# CMakeList.txt : Top-level CMake project file, do global configuration
# and include sub-projects here.
#
cmake_minimum_required (VERSION 3.17)


add_executable(
	Phantom.ProtoStore.Utility
	"main.cpp" 
	"DumpPartition.cpp"
	)

target_compile_definitions(
	Phantom.ProtoStore.Utility
	PUBLIC
	"BOOST_ALL_NO_LIB=1"
)

target_link_libraries(
	Phantom.ProtoStore.Utility
	Phantom.System
	Phantom.ProtoStore
	mimalloc-static
)
