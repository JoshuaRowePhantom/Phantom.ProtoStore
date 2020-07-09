cmake_minimum_required (VERSION 3.17)
find_package(cppcoro CONFIG REQUIRED)

add_library (
	Phantom.System
	STATIC
	"pooled_ptr.h" 
	"ThreadingProvider.cpp"
	"ThreadingProvider.h" 
	"utility.h" )

target_link_libraries(
	Phantom.System
	cppcoro
)

target_include_directories(
	Phantom.System
	PUBLIC
	..
)
