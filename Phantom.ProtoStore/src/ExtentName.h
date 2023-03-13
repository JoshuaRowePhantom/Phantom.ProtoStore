#pragma once

#include <functional>
#include "StandardTypes.h"

namespace Phantom::ProtoStore
{
FlatBuffers::ExtentNameT MakeDatabaseHeaderExtentName(
    uint64_t copyNumber);

FlatBuffers::ExtentNameT MakeLogExtentName(
    uint64_t logExtentSequenceNumber);

FlatBuffers::ExtentNameT MakePartitionHeaderExtentName(
    IndexNumber indexNumber,
    PartitionNumber partitionNumber,
    LevelNumber levelNumber,
    std::string indexName);

FlatBuffers::ExtentNameT MakePartitionDataExtentName(
    const FlatBuffers::IndexHeaderExtentName* partitionHeaderExtentName);

FlatBuffers::ExtentNameT MakeExtentName(
    const FlatBuffers::IndexHeaderExtentName* partitionHeaderExtentName);

FlatValue<ExtentName> Clone(
    const ExtentName* extentName);

}
