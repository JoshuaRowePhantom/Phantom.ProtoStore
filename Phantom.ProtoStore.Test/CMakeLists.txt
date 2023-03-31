# CMakeList.txt : Top-level CMake project file, do global configuration
# and include sub-projects here.
#
cmake_minimum_required (VERSION 3.24)

find_package(mimalloc REQUIRED)

add_executable(
    Phantom.ProtoStore.Test
    "async_test.cpp"
    "async_test.h"
    "BloomFilterTests.cpp" 
    "ChecksumTests.cpp" 
    "DataReferenceTests.cpp"
    "DelayedMemoryTableTransactionOutcomeTests.cpp" 
    "ExistingPartitionsTests.cpp"
    "ExtentStoreTests.cpp"
    "ExtentStoreTests.h" 
    "FlatBufferPointerKeyComparerTests.cpp"
    "HeaderAccessorTests.cpp"
    "IndexPartitionMergeGeneratorTests.cpp"
    "KeyComparerTests.cpp"
    "MemoryMappedFileExtentStoreTests.cpp"
    "MemoryTableTests.cpp"
    "MessageStoreTests.cpp"
    "PartitionTests.cpp"
    "pch.h"
    "ProtocolBuffersValueComparerTests.cpp"
    "ProtoStoreFlatBufferTests.cpp"
    "ProtoStoreProtocolBufferTests.cpp"
    "ProtoStoreTest.proto"
    "ProtoValueTests.cpp"
    "Resources.cpp"
    "Resources.h"
    "RowMergerTests.cpp"
    "SchemaTests.cpp"
    "SkipListTest.cpp"
    "StandardIncludes.cpp"
    "StandardIncludes.h"
    "TestFactories.cpp"
    "TestFactories.h" 
    "ValueBuilderTests.cpp"
)
    
PROTOBUF_GENERATE(
    LANGUAGE cpp
    PROTOC_OPTIONS -I "${CMAKE_CURRENT_SOURCE_DIR}/../Phantom.ProtoStore/include"
    TARGET Phantom.ProtoStore.Test
)

target_compile_definitions(
    Phantom.ProtoStore.Test
    PUBLIC
    "BOOST_ALL_NO_LIB=1"
    "$<$<CONFIG:RelWithDebInfo>:PHANTOM_USE_MIMALLOC=1>"
)

flatbuffers_generate_headers(
    TARGET Phantom.ProtoStore.Test.FlatBuffers
    INCLUDE_PREFIX Phantom.ProtoStore
    INCLUDE ../Phantom.ProtoStore/src ../Phantom.ProtoStore/include
    FLAGS --reflect-names --gen-name-strings --gen-compare --cpp-std c++17 --gen-object-api --scoped-enums --gen-mutable
    BINARY_SCHEMAS_DIR "${CMAKE_CURRENT_BINARY_DIR}"
    SCHEMAS
    ProtoStoreTest.fbs)

phantom_flatbuffers_generate_natvis(
    TARGET Phantom.ProtoStore.Test.FlatBuffers
    NATVIS_DIR "${CMAKE_CURRENT_SOURCE_DIR}"
)

cmrc_add_resource_library(
    Phantom.ProtoStore.Test.Resources
    NAMESPACE Phantom::ProtoStore::Test::Resources
)

phantom_add_flatbuffers_schema_resources(
    Phantom.ProtoStore.Test.FlatBuffers
    Phantom.ProtoStore.Test.Resources
    "${CMAKE_CURRENT_BINARY_DIR}"
)

target_link_libraries(
    Phantom.ProtoStore.Test
    PRIVATE
    "$<$<CONFIG:RelWithDebInfo>:mimalloc>")

target_link_libraries(
    Phantom.ProtoStore.Test
    PRIVATE
    Phantom.System
    Phantom.ProtoStore
    Phantom.ProtoStore.Test.Resources
    Phantom.ProtoStore.Test.FlatBuffers
    GTest::gtest
    GTest::gtest_main
)

target_include_directories(
    Phantom.ProtoStore.Test
    PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
)

target_precompile_headers(
    Phantom.ProtoStore.Test
    PRIVATE
    "pch.h"
)

gtest_discover_tests(
    Phantom.ProtoStore.Test
    EXTRA_ARGS --gtest_break_on_failure=1)
