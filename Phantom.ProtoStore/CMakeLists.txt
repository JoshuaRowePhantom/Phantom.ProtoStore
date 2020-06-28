# CMakeList.txt : CMake project for Phantom.ProtoStore, include source and define
# project specific logic here.
#
cmake_minimum_required (VERSION 3.8)

find_package(Protobuf REQUIRED)

include_directories(${Protobuf_INCLUDE_DIRS})

protobuf_generate_cpp(
	PROTOC_CPP
	PROTOC_H
	ProtoStore.proto
)

# Add source to this project's executable.
add_library (
	Phantom.ProtoStore 
	SHARED
	"Phantom.ProtoStore.cpp"
	"Phantom.ProtoStore.h"
	${PROTOC_CPP}
	${PROTOC_H})

	
target_link_libraries(
	Phantom.ProtoStore
	${Protobuf_LIBRARIES}
)

# TODO: Add tests and install targets if needed.
