# CMakeList.txt : Top-level CMake project file, do global configuration
# and include sub-projects here.
#
cmake_minimum_required (VERSION 3.17)

find_package(mimalloc REQUIRED)

add_executable(
    Phantom.ProtoStore.Test
    "async_test.cpp"
    "async_test.h"
    "BloomFilterTests.cpp" 
    "ChecksumTests.cpp" 
    "DataReferenceTests.cpp"
    "DelayedMemoryTableTransactionOutcomeTests.cpp" 
    "ExtentStoreTests.cpp"
    "ExtentStoreTests.h" 
    "HeaderAccessorTests.cpp"
    "KeyComparerTests.cpp"
    "lifetime_tracker.h"
    "MemoryExtentStoreTests.cpp"
    "MemoryMappedFileExtentStoreTests.cpp"
    "MemoryTableTests.cpp"
    "MessageStoreTests.cpp"
    "PartitionTests.cpp"
    "ProtoStoreTest.proto"
    "ProtoStoreTests.cpp"
    "ProtoValueTests.cpp"
    "RowMergerTests.cpp"
    "SchemaTests.cpp"
    "SkipListTest.cpp"
    "StandardIncludes.cpp"
    "StandardIncludes.h"
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
)

flatbuffers_generate_headers(
    TARGET Phantom.ProtoStore.Test.FlatBuffers
    INCLUDE_PREFIX . ../Phantom.ProtoStore/src ../Phantom.ProtoStore/include
    FLAGS --reflect-names --gen-name-strings --gen-compare --cpp-std c++17 --gen-object-api --scoped-enums --gen-mutable
    SCHEMAS
    ProtoStoreTest.fbs)

target_link_libraries(
    Phantom.ProtoStore.Test
    "$<$<CONFIG:RelWithDebInfo>:mimalloc>")

target_link_libraries(
    Phantom.ProtoStore.Test
    Phantom.System
    Phantom.ProtoStore
    Phantom.ProtoStore.FlatBuffers
    Phantom.ProtoStore.Test.FlatBuffers
    GTest::gtest
    GTest::gtest_main
)

target_include_directories(
    Phantom.ProtoStore.Test
    PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
)

gtest_discover_tests(
    Phantom.ProtoStore.Test
    EXTRA_ARGS --gtest_break_on_failure=1)
