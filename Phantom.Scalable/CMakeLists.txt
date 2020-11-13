cmake_minimum_required (VERSION 3.17)

get_target_property(
    protobuf_INCLUDE_DIRECTORIES
    protobuf::libprotoc
    INCLUDE_DIRECTORIES
)

phantom_grpc_generate_cpp(
    GRPC_CPP
    GRPC_H
    IMPORTDIRECTORY
    include
    ${protobuf_INCLUDE_DIRECTORIES}
    ${CMAKE_CURRENT_SOURCE_DIR}
    PROTO
    Phantom.Scalable/PhantomScalableGrpc.proto
    src/PhantomScalableGrpcInternal.proto
)

add_library (
    Phantom.Scalable 
    STATIC
    src/Consensus.cpp
    include/Phantom.Scalable/Consensus.h
    src/Paxos.cpp
    include/Phantom.Scalable/Paxos.h
    src/StandardIncludes.cpp
    src/StandardIncludes.h
    ${GRPC_CPP}
    ${GRPC_H} 
    )
    
    
target_include_directories(
    Phantom.Scalable
    PUBLIC
    include
    ${Boost_INCLUDE_DIR}
    ${Protobuf_INCLUDE_DIRS}
    ${CMAKE_CURRENT_BINARY_DIR}
)

target_compile_definitions(
	Phantom.Scalable
    PUBLIC
	"BOOST_ALL_NO_LIB=1"
)

target_link_libraries(
    Phantom.Scalable
    PUBLIC
    cppcoro
    libprotobuf
    Phantom.ProtoStore
    Phantom.System
    boost_crc
    boost_filesystem
    boost_interprocess
    grpc
)
