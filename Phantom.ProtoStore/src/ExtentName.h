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
