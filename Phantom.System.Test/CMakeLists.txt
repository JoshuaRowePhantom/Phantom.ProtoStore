find_package(GTest REQUIRED)

add_executable(
	Phantom.System.Test
	"ThreadingTests.cpp"
	"ThreadingTests.h"
)

target_link_libraries(
	Phantom.System.Test
	Phantom.System
	${GTEST_LIBRARIES}
	GTest::Main
)

gtest_discover_tests(Phantom.System.Test)
