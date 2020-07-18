# CMakeList.txt : Top-level CMake project file, do global configuration
# and include sub-projects here.
#
cmake_minimum_required (VERSION 3.17)

phantom_protobuf_generate_cpp(
    PROTOC_CPP
    PROTOC_H
    IMPORTDIRECTORY
	${Protobuf_INCLUDE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
	../Phantom.ProtoStore/include
	PROTO
    ProtoStoreTest.proto
)

add_executable(
	Phantom.ProtoStore.Test
	"BloomFilterTests.cpp" 
	"ChecksumTests.cpp" 
	"HeaderAccessorTests.cpp"
	"MemoryExtentStoreTests.cpp"
	"KeyComparerTests.cpp"
	"MemoryTableTests.cpp"
	"MessageStoreTests.cpp"
	"ProtoStoreTests.cpp"
	"ProtoValueTests.cpp"
	"SchemaTests.cpp"
	"SkipListTest.cpp"
	"StandardIncludes.cpp"
	"StandardIncludes.h"
	${PROTOC_CPP}
    ${PROTOC_H})

target_link_libraries(
	Phantom.ProtoStore.Test
	Phantom.System
	Phantom.ProtoStore
	gtest
	gtest_main
)

target_include_directories(
	Phantom.ProtoStore.Test
	PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
)

gtest_discover_tests(
	Phantom.ProtoStore.Test)
