cmake_minimum_required (VERSION 3.17)

add_library (
	Phantom.System
	STATIC
	"async_reader_writer_lock.h"
	"async_value_source.h"
	"empty.cpp"
	"merge.h"
	"pooled_ptr.h" 
	"put_back.h"
	"stable_random.h"
	"ThreadingProvider.cpp"
	"ThreadingProvider.h" 
	"utility.h")

target_link_libraries(
	Phantom.System
	cppcoro
)

target_include_directories(
	Phantom.System
	PUBLIC
	..
)
