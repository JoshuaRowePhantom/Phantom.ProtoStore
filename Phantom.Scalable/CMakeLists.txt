cmake_minimum_required (VERSION 3.24)

add_library (
    Phantom.Scalable 
    STATIC
    include/Phantom.Scalable/PhantomScalable.fbs
    "src/Consensus.cpp"
    "src/Entrypoint.cpp"
    "src/PhantomScalableInternal.fbs"
)
    
flatbuffers_generate_headers(
    TARGET Phantom.Scalable.FlatBuffers
    INCLUDE_PREFIX Phantom.Scalable
    INCLUDE include src
    FLAGS --reflect-names --gen-name-strings --gen-compare --cpp-std c++17 --gen-object-api --scoped-enums --gen-mutable
    BINARY_SCHEMAS_DIR "${CMAKE_CURRENT_BINARY_DIR}"
    SCHEMAS
    include/Phantom.Scalable/PhantomScalable.fbs
    src/PhantomScalableInternal.fbs
    )

phantom_flatbuffers_generate_natvis(
    TARGET Phantom.Scalable.FlatBuffers
    NATVIS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/FlatBuffersNatvis"
)

target_include_directories(
    Phantom.Scalable
    PUBLIC
    include
    ${Boost_INCLUDE_DIR}
    ${Protobuf_INCLUDE_DIRS}
    ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_CURRENT_BINARY_DIR}/src
    ${CMAKE_CURRENT_BINARY_DIR}/include
)

target_compile_definitions(
    Phantom.Scalable
    PUBLIC
    "BOOST_ALL_NO_LIB=1"
)

target_link_libraries(
    Phantom.Scalable
    PUBLIC
    Phantom.ProtoStore
    Phantom.System
    Phantom.Coroutines
)
