cmake_minimum_required (VERSION 3.17)

find_package(Protobuf REQUIRED)
find_package(cppcoro CONFIG REQUIRED)

protobuf_generate_cpp(
    PROTOC_CPP
    PROTOC_H
    src/ProtoStore.proto
)

add_library (
    Phantom.ProtoStore 
    STATIC
    "src/Checksum.cpp"
    "src/Checksum.h"
    
    "src/ExtentStore.h"
    "src/HeaderAccessor.cpp"
    "src/HeaderAccessor.h"
    "src/HeaderAccessorImpl.h"
    "src/MemoryExtentStore.cpp"
    "src/MemoryExtentStore.h"
    "src/MessageStore.cpp"
    "src/MessageStore.h" 
    "src/MessageStoreImpl.h"
    "src/Phantom.ProtoStore.cpp"
    "src/ProtoStore.cpp"
    "src/ProtoStore.h"
    "src/ProtoStoreFactory.cpp"
    "src/ProtoStoreFactory.h"
    "src/RandomMessageAccessor.cpp"
    "src/RandomMessageAccessor.h"
    "src/RandomMessageAccessorImpl.h"
    "src/StandardTypes.h"
    "include/Phantom.ProtoStore.h"
    ${PROTOC_CPP}
    ${PROTOC_H} "src/ProtoStore.h")
    
    
target_include_directories(
    Phantom.ProtoStore
    PUBLIC
    ${Protobuf_INCLUDE_DIRS}
    ..
    PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
)

target_link_libraries(
    Phantom.ProtoStore
    ${Protobuf_LIBRARIES}
    Phantom.System
)
