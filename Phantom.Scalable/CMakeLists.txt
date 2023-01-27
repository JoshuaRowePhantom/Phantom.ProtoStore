cmake_minimum_required (VERSION 3.17)

add_library (
    Phantom.Scalable 
    STATIC
    include/Phantom.Scalable/AnyMessage.h
    include/Phantom.Scalable/Consensus.h
    include/Phantom.Scalable/DataComponentString.h
    include/Phantom.Scalable/EgalitarianPaxos.h
    include/Phantom.Scalable/EgalitarianPaxosImplementation.h
    include/Phantom.Scalable/InternalLockResourceManager.h
    include/Phantom.Scalable/InternalLockResourceManagerDatabase.h
    include/Phantom.Scalable/InternalResourceManager.h
    include/Phantom.Scalable/InternalTransaction.h
    include/Phantom.Scalable/InternalTransactionImpl.h
    include/Phantom.Scalable/LockResourceManager.h
    include/Phantom.Scalable/Memory.h
    include/Phantom.Scalable/Paxos.h
    include/Phantom.Scalable/PeerToPeerClient.h
    include/Phantom.Scalable/PeerToPeerService.h
    include/Phantom.Scalable/ResourceManager.h
    include/Phantom.Scalable/ScalablePaxos.h
    include/Phantom.Scalable/Scheduling.h
    include/Phantom.Scalable/StandardIncludes.h
    src/Consensus.cpp
    src/EgalitarianPaxos.cpp
    src/InternalLockResourceManager.cpp
    src/InternalLockResourceManagerDatabase.cpp
    src/InternalResourceManager.cpp
    src/InternalTransaction.cpp
    src/LockResourceManager.cpp
    src/Paxos.cpp
    src/PeerToPeerClient.cpp
    src/PeerToPeerService.cpp
    src/ResourceManager.cpp
    src/ScalablePaxos.cpp
    src/Scheduling.cpp
    src/StandardIncludes.cpp
    src/StandardIncludes.h
    )
    

PROTOBUF_GENERATE(
    LANGUAGE cpp
    TARGET Phantom.Scalable
    PROTOS 
    include/Phantom.Scalable/PhantomScalableGrpc.proto
    src/PhantomScalableGrpcInternal.proto
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
    libprotobuf
    Phantom.ProtoStore
    Phantom.System
    boost_crc
    boost_filesystem
    boost_interprocess
    grpc
    Phantom.Coroutines
)
