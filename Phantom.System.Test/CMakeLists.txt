
add_executable(
	Phantom.System.Test
	"async_reader_writer_lock_tests.cpp"
	"async_value_source_tests.cpp"
	"pooled_ptr_tests.cpp" 
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
