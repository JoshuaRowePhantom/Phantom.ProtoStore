cmake_minimum_required (VERSION 3.17)

add_library (
    Phantom.ProtoStore 
    STATIC
    "src/BloomFilter.h"
    "src/Checksum.cpp"
    "src/Checksum.h"
    "src/ExtentManager.h"
    "src/ExtentName.cpp"
    "src/ExtentName.h"
    "src/ExtentStore.h"
    "src/HeaderAccessor.cpp"
    "src/HeaderAccessor.h"
    "src/HeaderAccessorImpl.h"
    "src/Index.cpp"
    "src/Index.h"
    "src/IndexImpl.h"
    "src/IndexDataSources.cpp"
    "src/IndexDataSources.h"
    "src/IndexDataSourcesImpl.h"
    "src/IndexMerger.cpp"
    "src/IndexMerger.h"
    "src/IndexPartitionMergeGenerator.cpp"
    "src/IndexPartitionMergeGenerator.h"
    "src/InternalProtoStore.h"
    "src/KeyComparer.cpp"
    "src/KeyComparer.h"
    "src/LogManager.cpp"
    "src/LogManager.h"
    "src/MemoryExtentStore.cpp"
    "src/MemoryExtentStore.h"
    "src/MemoryMappedFileExtentStore.cpp"
    "src/MemoryMappedFileExtentStore.h"
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
    src/ProtoStoreInternal.proto
    include/Phantom.ProtoStore/ProtoStore.proto
    )
    

target_include_directories(
    Phantom.ProtoStore
    PUBLIC
    include
    ${Boost_INCLUDE_DIR}
    ${Protobuf_INCLUDE_DIRS}
    ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_CURRENT_BINARY_DIR}/src
    ${CMAKE_CURRENT_BINARY_DIR}/include
)

PROTOBUF_GENERATE(
    LANGUAGE cpp
    TARGET Phantom.ProtoStore 
    IMPORT_DIRS ./src ./include
    PROTOS
    src/ProtoStoreInternal.proto
    include/Phantom.ProtoStore/ProtoStore.proto
)

target_compile_definitions(
    Phantom.ProtoStore
    PUBLIC
    "BOOST_ALL_NO_LIB=1"
)

target_link_libraries(
    Phantom.ProtoStore
    PUBLIC
    Phantom.Coroutines.cppcoro
    protobuf::libprotobuf
    PRIVATE
    Phantom.System
)
