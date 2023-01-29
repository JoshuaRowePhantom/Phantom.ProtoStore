# CMakeList.txt : Top-level CMake project file, do global configuration
# and include sub-projects here.
#
cmake_minimum_required (VERSION 3.17)

find_package(mimalloc REQUIRED)

add_executable(
    Phantom.ProtoStore.Test
    "BloomFilterTests.cpp" 
    "ChecksumTests.cpp" 
    "HeaderAccessorTests.cpp"
    "KeyComparerTests.cpp"
    "MemoryExtentStoreTests.cpp"
    "MemoryMappedFileExtentStoreTests.cpp"
    "MemoryTableTests.cpp"
    "MessageStoreTests.cpp"
    "PartitionTests.cpp"
    "ProtoStoreTests.cpp"
    "ProtoValueTests.cpp"
    "RowMergerTests.cpp"
    "SchemaTests.cpp"
    "SkipListTest.cpp"
    "StandardIncludes.cpp"
    "StandardIncludes.h"
    ProtoStoreTest.proto
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

target_link_libraries(
    Phantom.ProtoStore.Test
    "$<$<CONFIG:RelWithDebInfo>:mimalloc>")

target_link_libraries(
    Phantom.ProtoStore.Test
    Phantom.System
    Phantom.ProtoStore
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
