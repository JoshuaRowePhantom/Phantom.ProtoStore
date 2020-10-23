# CMakeList.txt : Top-level CMake project file, do global configuration
# and include sub-projects here.
#
cmake_minimum_required (VERSION 3.17)

get_target_property(
    protobuf_INCLUDE_DIRECTORIES
    protobuf::libprotoc
    INCLUDE_DIRECTORIES
)

#phantom_protobuf_generate_cpp(
#    PROTOC_CPP
#    PROTOC_H
#    IMPORTDIRECTORY
#    ${protobuf_INCLUDE_DIRECTORIES}
#    ${CMAKE_CURRENT_SOURCE_DIR}
#	../Phantom.ProtoStore/include
#	PROTO
#    ProtoStoreTest.proto
#)

add_executable(
	Phantom.Scalable.Test
	ConsensusTests.cpp
	"StandardIncludes.cpp"
	"StandardIncludes.h"
	${PROTOC_CPP}
    ${PROTOC_H})

target_compile_definitions(
	Phantom.Scalable.Test
	PUBLIC
	"BOOST_ALL_NO_LIB=1"
)

target_link_libraries(
	Phantom.Scalable.Test
	PUBLIC
	Phantom.System
	Phantom.ProtoStore
	Phantom.Scalable
	gtest
	gtest_main
	mimalloc-static
)

target_include_directories(
	Phantom.Scalable.Test
	PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
)

gtest_discover_tests(
	Phantom.Scalable.Test
	EXTRA_ARGS --gtest_break_on_failure=1)
