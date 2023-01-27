cmake_minimum_required (VERSION 3.17)

add_library (
	Phantom.System
	STATIC
	"async_ring_buffer.h"
	"async_reader_writer_lock.cpp"
	"async_reader_writer_lock.h"
	"async_scoped_lock.h"
	"async_utility.h"
	"async_value_source.h"
	"empty.cpp"
	"encompassing_pending_task.h"
	"functional.h"
	"merge.h"
	"pooled_ptr.h" 
	"put_back.h"
	"scope.h"
	"service_provider.h"
	"single_pending_task.h"
	"stable_random.h"
	"ThreadingProvider.cpp"
	"ThreadingProvider.h" 
	"utility.h")

target_link_libraries(
	Phantom.System
	Phantom.Coroutines
	Phantom.Coroutines.cppcoro
)

target_include_directories(
	Phantom.System
	PUBLIC
	..
)
