# CMakeList.txt : Top-level CMake project file, do global configuration
# and include sub-projects here.
#
cmake_minimum_required (VERSION 3.17)
find_package(GTest REQUIRED)

add_executable(
	Phantom.ProtoStore.Test
	"ProtoStoreTests.cpp"
)

target_link_libraries(
	Phantom.ProtoStore.Test
	Phantom.System
	Phantom.ProtoStore
	${GTEST_LIBRARIES}
	GTest::Main
)

gtest_discover_tests(Phantom.ProtoStore.Test)
