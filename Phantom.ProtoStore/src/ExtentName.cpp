#include "StandardTypes.h"
#include "ExtentName.h"
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"
#include <boost/functional/hash.hpp>

namespace Phantom::ProtoStore
{

ExtentNameT MakeDatabaseHeaderExtentName(
    uint64_t copyNumber)
{
    ExtentNameT extentName;
    FlatBuffers::DatabaseHeaderExtentNameT databaseHeaderExtentName;
    databaseHeaderExtentName.header_copy_number = copyNumber;
    extentName.extent_name.Set(databaseHeaderExtentName);
    return std::move(extentName);
}

ExtentNameT MakeLogExtentName(
    uint64_t logExtentSequenceNumber)
{
    ExtentNameT extentName;
    FlatBuffers::LogExtentNameT logExtentName;
    logExtentName.log_extent_sequence_number = logExtentSequenceNumber;
    extentName.extent_name.Set(logExtentName);
    return std::move(extentName);
}

ExtentNameT MakePartitionDataExtentName(
    const FlatBuffers::IndexHeaderExtentName* partitionHeaderExtentName)
{
    ExtentNameT extentName;
    FlatBuffers::IndexDataExtentNameT indexDataExtentName;
    indexDataExtentName.index_extent_name.reset(
        partitionHeaderExtentName->index_extent_name()->UnPack());
    extentName.extent_name.Set(indexDataExtentName);
    return std::move(extentName);
}

ExtentNameT MakePartitionHeaderExtentName(
    IndexNumber indexNumber,
    PartitionNumber partitionNumber,
    LevelNumber levelNumber,
    std::string indexName)
{
    ExtentNameT extentName;
    FlatBuffers::IndexExtentNameT indexExtentName;
    indexExtentName.index_number = indexNumber;
    indexExtentName.partition_number = partitionNumber;
    indexExtentName.level = levelNumber;
    indexExtentName.index_name = std::move(indexName);
    FlatBuffers::IndexHeaderExtentNameT indexHeaderExtentName;
    indexHeaderExtentName.index_extent_name = copy_unique(indexExtentName);
    extentName.extent_name.Set(indexHeaderExtentName);
    return std::move(extentName);
}

FlatBuffers::ExtentNameT MakeExtentName(
    const FlatBuffers::IndexHeaderExtentName* partitionHeaderExtentName)
{
    ExtentNameT extentName;
    extentName.extent_name.Set(FlatBuffers::IndexHeaderExtentNameT());
    partitionHeaderExtentName->UnPackTo(extentName.extent_name.AsIndexHeaderExtentName());
    return std::move(extentName);
}

FlatValue<ExtentName> Clone(
    const ExtentName* extentName
)
{
    return FlatValue(extentName).Clone(
        *FlatBuffersSchemas::ProtoStoreSchema,
        *FlatBuffersSchemas::ExtentName_Object
    );
}
}
