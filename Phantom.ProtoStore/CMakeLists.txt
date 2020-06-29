cmake_minimum_required (VERSION 3.17)

find_package(Protobuf REQUIRED)

include_directories(
	${Protobuf_INCLUDE_DIRS}
)

protobuf_generate_cpp(
	PROTOC_CPP
	PROTOC_H
	ProtoStore.proto
)

add_library (
	Phantom.ProtoStore 
	STATIC
	"Phantom.ProtoStore.cpp"
	"Phantom.ProtoStore.h"
	${PROTOC_CPP}
	${PROTOC_H}
)
	
target_link_libraries(
	Phantom.ProtoStore
	${Protobuf_LIBRARIES}
)
