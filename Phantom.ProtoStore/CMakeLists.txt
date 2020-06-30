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
    "src/ExtentStore.cpp"
    "src/ExtentStore.h"
    "src/MemoryExtentStore.cpp"
    "src/MemoryExtentStore.h"
    "src/MessageStore.h" 
    "src/Phantom.ProtoStore.cpp"
    "src/ProtoStore.cpp"
    "include/Phantom.ProtoStore.h"
    ${PROTOC_CPP}
    ${PROTOC_H} 
    )
    
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
