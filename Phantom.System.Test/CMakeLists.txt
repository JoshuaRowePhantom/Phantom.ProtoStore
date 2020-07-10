find_package(GTest REQUIRED)
find_package(cppcoro CONFIG REQUIRED)

add_executable(
	Phantom.System.Test
	"async_value_source_tests.cpp"
	"pooled_ptr_tests.cpp" 
	"StandardIncludes.h"
	"ThreadingTests.cpp"
	"ThreadingTests.h"
	)

target_link_libraries(
	Phantom.System.Test
	Phantom.System
	${GTEST_LIBRARIES}
	GTest::Main
	cppcoro
)

gtest_discover_tests(Phantom.System.Test)
