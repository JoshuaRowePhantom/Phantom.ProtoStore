cmake_minimum_required (VERSION 3.17)

phantom_protobuf_generate_cpp(
    PROTOC_CPP
    PROTOC_H
    IMPORTDIRECTORY
    include
    ${Protobuf_INCLUDE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
    PROTO
    Phantom.ProtoStore/ProtoStore.proto
    src/ProtoStoreInternal.proto
)

add_library (
    Phantom.ProtoStore 
    STATIC
    "src/BloomFilter.h"
    "src/Checksum.cpp"
    "src/Checksum.h"
    "src/ExtentManager.h"
    "src/ExtentStore.h"
    "src/HeaderAccessor.cpp"
    "src/HeaderAccessor.h"
    "src/HeaderAccessorImpl.h"
    "src/Index.cpp"
    "src/Index.h"
    "src/IndexImpl.h"
    "src/KeyComparer.cpp"
    "src/KeyComparer.h"
    "src/MemoryExtentStore.cpp"
    "src/MemoryExtentStore.h"
    "src/MemoryTable.cpp" 
    "src/MemoryTable.h"
    "src/MemoryTableImpl.h"
    "src/MessageStore.cpp"
    "src/MessageStore.h" 
    "src/MessageStoreImpl.h"
    "src/Partition.cpp"
    "src/Partition.h"
    "src/PartitionImpl.h"
    "src/PartitionTreeNodeCache.cpp"
    "src/PartitionTreeNodeCache.h"
    "src/PartitionWriter.cpp"
    "src/PartitionWriter.h"
    "src/PartitionWriterImpl.h"
    "src/Phantom.ProtoStore.cpp"
    "src/ProtoStore.cpp"
    "src/ProtoStore.h"
    "src/ProtoStoreFactory.cpp"
    "src/ProtoStoreFactory.h"
    "src/RandomMessageAccessor.cpp"
    "src/RandomMessageAccessor.h"
    "src/RandomMessageAccessorImpl.h"
    "src/RowMerger.cpp"
    "src/RowMerger.h"
    "src/Schema.cpp"
    "src/Schema.h"
    "src/SkipList.cpp"
    "src/SkipList.h" 
    "src/StandardTypes.h"
    "include/Phantom.ProtoStore/Phantom.ProtoStore.h"
    ${PROTOC_CPP}
    ${PROTOC_H} )
    
    
target_include_directories(
    Phantom.ProtoStore
    PUBLIC
    include
    ${Boost_INCLUDE_DIR}
    ${Protobuf_INCLUDE_DIRS}
    ${CMAKE_CURRENT_BINARY_DIR}
)

target_link_libraries(
    Phantom.ProtoStore
    PUBLIC
    cppcoro
    libprotobuf
    PRIVATE
    Phantom.System
    boost_crc
)
