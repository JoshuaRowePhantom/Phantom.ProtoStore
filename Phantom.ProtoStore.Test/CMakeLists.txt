# CMakeList.txt : Top-level CMake project file, do global configuration
# and include sub-projects here.
#
cmake_minimum_required (VERSION 3.17)
find_package(GTest REQUIRED)

add_executable(
	Phantom.ProtoStore.Test
	"ProtoStoreTests.cpp"
	"MemoryExtentStoreTest.cpp")

target_link_libraries(
	Phantom.ProtoStore.Test
	Phantom.System
	Phantom.ProtoStore
	${GTEST_LIBRARIES}
	GTest::Main
)

target_include_directories(
	Phantom.ProtoStore.Test
	PRIVATE
	$<TARGET_PROPERTY:Phantom.ProtoStore,INCLUDE_DIRECTORIES>
)

gtest_discover_tests(Phantom.ProtoStore.Test)
