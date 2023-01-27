# CMakeList.txt : Top-level CMake project file, do global configuration
# and include sub-projects here.
#
cmake_minimum_required (VERSION 3.17)


add_executable(
	Phantom.ProtoStore.Test
	"BloomFilterTests.cpp" 
	"ChecksumTests.cpp" 
	"HeaderAccessorTests.cpp"
	"KeyComparerTests.cpp"
	"MemoryExtentStoreTests.cpp"
	"MemoryMappedFileExtentStoreTests.cpp"
	"MemoryTableTests.cpp"
	"MessageStoreTests.cpp"
	"PartitionTests.cpp"
	"ProtoStoreTests.cpp"
	"ProtoValueTests.cpp"
	"RowMergerTests.cpp"
	"SchemaTests.cpp"
	"SkipListTest.cpp"
	"StandardIncludes.cpp"
	"StandardIncludes.h")
	
PROTOBUF_GENERATE(
    LANGUAGE cpp
    TARGET Phantom.ProtoStore.Test
    PROTOS 
    ProtoStoreTest.proto
)

target_compile_definitions(
	Phantom.ProtoStore.Test
	PUBLIC
	"BOOST_ALL_NO_LIB=1"
)

target_link_libraries(
	Phantom.ProtoStore.Test
	Phantom.System
	Phantom.ProtoStore
	gtest
	gtest_main
	mimalloc-static
)

target_include_directories(
	Phantom.ProtoStore.Test
	PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
)

gtest_discover_tests(
	Phantom.ProtoStore.Test
	EXTRA_ARGS --gtest_break_on_failure=1)
