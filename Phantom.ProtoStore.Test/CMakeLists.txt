# CMakeList.txt : Top-level CMake project file, do global configuration
# and include sub-projects here.
#
cmake_minimum_required (VERSION 3.17)
find_package(Protobuf REQUIRED)
find_package(GTest REQUIRED)

protobuf_generate_cpp(
    PROTOC_CPP
    PROTOC_H
    ProtoStoreTest.proto
)

add_executable(
	Phantom.ProtoStore.Test
	"ChecksumTests.cpp" 
	"HeaderAccessorTests.cpp"
	"MemoryExtentStoreTests.cpp"
	"MessageStoreTests.cpp"
	"ProtoStoreTests.cpp"
	"ProtoValueTests.cpp"
	${PROTOC_CPP}
    ${PROTOC_H})

target_link_libraries(
	Phantom.ProtoStore.Test
	Phantom.System
	Phantom.ProtoStore
	${GTEST_LIBRARIES}
	${Protobuf_LIBRARIES}
	GTest::Main
)

target_include_directories(
	Phantom.ProtoStore.Test
	PRIVATE
	$<TARGET_PROPERTY:Phantom.ProtoStore,INCLUDE_DIRECTORIES>
    ${CMAKE_CURRENT_BINARY_DIR}
)

gtest_discover_tests(
	Phantom.ProtoStore.Test)
