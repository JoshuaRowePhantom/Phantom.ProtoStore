
add_executable(
	Phantom.System.Test
	"as_awaitable_tests.cpp"
	"async_reader_writer_lock_tests.cpp"
	"async_value_source_tests.cpp"
	"encompassing_pending_task_tests.cpp"
	"merge_tests.cpp"
	"pooled_ptr_tests.cpp" 
	"single_pending_task_tests.cpp" 
	"StandardIncludes.h"
	"ThreadingTests.cpp"
	"ThreadingTests.h"
	 )

target_link_libraries(
	Phantom.System.Test
	Phantom.System
	gtest
	gtest_main
	cppcoro
)

target_include_directories(
	Phantom.System.Test
	PRIVATE
	gtest
)

gtest_discover_tests(Phantom.System.Test)
