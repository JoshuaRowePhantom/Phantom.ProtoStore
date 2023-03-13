cmake_minimum_required (VERSION 3.24)

add_library (
    Phantom.ProtoStore 
    STATIC
    "include/Phantom.ProtoStore/Async.h"
    "include/Phantom.ProtoStore/Errors.h"
    "include/Phantom.ProtoStore/Payloads.h"  
    "include/Phantom.ProtoStore/Phantom.ProtoStore.h"
    "include/Phantom.ProtoStore/Primitives.h"
    "include/Phantom.ProtoStore/ProtoIndex.h"
    "include/Phantom.ProtoStore/ProtoStore.proto"
    "include/Phantom.ProtoStore/Scheduler.h" 
    "include/Phantom.ProtoStore/Schema.h"
    "src/BloomFilter.h"
    "src/CanonicalProtoKey.cpp"
    "src/CanonicalProtoKey.h" 
    "src/Checksum.cpp"
    "src/Checksum.h"
    "src/Errors.cpp"
    "src/ExtentManager.h"
    "src/ExtentName.cpp"
    "src/ExtentName.h"
    "src/ExtentStore.h"
    "src/FlatBuffersKeyComparer.cpp"
    "src/FlatBuffersKeyComparer.h"
    "src/FlatMessage.h"
    "src/HeaderAccessor.cpp"
    "src/HeaderAccessor.h"
    "src/HeaderAccessorImpl.h"
    "src/Index.cpp"
    "src/Index.h"
    "src/IndexDataSources.cpp"
    "src/IndexDataSources.h"
    "src/IndexDataSourcesImpl.h"
    "src/IndexImpl.h"
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
    "src/PartitionWriter.cpp"
    "src/PartitionWriter.h"
    "src/PartitionWriterImpl.h"
    "src/Payloads.cpp"
    "src/pch.h"
    "src/Phantom.ProtoStore.cpp"
    "src/ProtoStore.cpp"
    "src/ProtoStore.h"
    "src/ProtoStoreFactory.cpp"
    "src/ProtoStoreFactory.h"
    "src/ProtoStoreInternal.proto"
    "src/Resources.cpp" 
    "src/Resources.h"
    "src/RowMerger.cpp"
    "src/RowMerger.h"
    "src/Scheduler.cpp"
    "src/Schema.cpp"
    "src/Schema.h"
    "src/SkipList.cpp"
    "src/SkipList.h" 
    "src/StandardTypes.cpp"
    "src/StandardTypes.h"
    "src/UnresolvedTransactionsTracker.cpp"
    "src/UnresolvedTransactionsTracker.h" 
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

flatbuffers_generate_headers(
    TARGET Phantom.ProtoStore.FlatBuffers
    INCLUDE_PREFIX Phantom.ProtoStore
    INCLUDE include
    FLAGS --reflect-names --gen-name-strings --gen-compare --cpp-std c++17 --gen-object-api --scoped-enums --gen-mutable
    BINARY_SCHEMAS_DIR "${CMAKE_CURRENT_BINARY_DIR}"
    SCHEMAS
    include/Phantom.ProtoStore/ProtoStore.fbs
    src/ProtoStoreInternal.fbs
    src/reflection-phantom-protostore.fbs)

phantom_flatbuffers_generate_natvis(
    TARGET Phantom.ProtoStore.FlatBuffers
    NATVIS_DIR "${CMAKE_CURRENT_SOURCE_DIR}"
)

cmrc_add_resource_library(
    Phantom.ProtoStore.Resources
    NAMESPACE Phantom::ProtoStore::Resources
)

phantom_add_flatbuffers_schema_resources(
    Phantom.ProtoStore.FlatBuffers
    Phantom.ProtoStore.Resources
    "${CMAKE_CURRENT_BINARY_DIR}"
)

target_compile_definitions(
    Phantom.ProtoStore
    PUBLIC
    "BOOST_ALL_NO_LIB=1"
)

target_precompile_headers(
    Phantom.ProtoStore
    PRIVATE
    "src/pch.h"
)

target_link_libraries(
    Phantom.ProtoStore
    PUBLIC
    flatbuffers::flatbuffers
    Phantom.Coroutines.cppcoro
    Phantom.ProtoStore.FlatBuffers
    Phantom.ProtoStore.Resources
    Phantom.System
    protobuf::libprotobuf
)
