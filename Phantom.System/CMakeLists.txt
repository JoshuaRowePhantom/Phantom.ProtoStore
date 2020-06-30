cmake_minimum_required (VERSION 3.17)
find_package(cppcoro CONFIG REQUIRED)

add_library (
	Phantom.System
	STATIC
	"ThreadingProvider.cpp"
	"ThreadingProvider.h")

target_link_libraries(
	Phantom.System
	cppcoro
)