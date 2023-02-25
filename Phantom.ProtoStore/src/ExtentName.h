#pragma once

#include <functional>
#include "StandardTypes.h"

namespace Phantom::ProtoStore
{
class ExtentName;

bool operator==(
    const ExtentName& left,
    const ExtentName& right
    ) noexcept;

ExtentName MakeDatabaseHeaderExtentName(
    uint64_t copyNumber);

ExtentName MakeLogExtentName(
    uint64_t logExtentSequenceNumber);

ExtentName MakePartitionHeaderExtentName(
    IndexNumber indexNumber,
    PartitionNumber partitionNumber,
    LevelNumber levelNumber,
    std::string indexName);

ExtentName MakePartitionDataExtentName(
    ExtentName partitionHeaderExtentName);

ExtentName MakeExtentName(
    const FlatBuffers::ExtentNameT& extentName
);

ExtentName MakeExtentName(
    const FlatBuffers::ExtentName& extentName
); 

FlatBuffers::ExtentNameT MakeExtentName(
    const ExtentName& extentName
);

flatbuffers::Offset<FlatBuffers::ExtentName> CreateExtentName(
    flatbuffers::FlatBufferBuilder&,
    const ExtentName&
);

}

namespace std
{
template<>
struct hash<Phantom::ProtoStore::ExtentName>
{
    size_t operator()(
        const Phantom::ProtoStore::ExtentName&) const noexcept;
};

}
