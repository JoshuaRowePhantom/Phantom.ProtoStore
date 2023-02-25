#include "StandardTypes.h"
#include "ExtentName.h"
#include "src/ProtoStoreInternal_generated.h"
#include <boost/functional/hash.hpp>

std::size_t std::hash<Phantom::ProtoStore::ExtentName>::operator()(
    const Phantom::ProtoStore::ExtentName& extentName) const noexcept
{
    using boost::hash_combine;
    size_t hash = 0;
    hash_combine(hash, extentName.has_databaseheaderextentname());
    hash_combine(hash, extentName.has_indexdataextentname());
    hash_combine(hash, extentName.has_indexheaderextentname());
    hash_combine(hash, extentName.has_logextentname());

    if (extentName.has_databaseheaderextentname())
    {
        hash_combine(hash, extentName.databaseheaderextentname().headercopynumber());
    }
    if (extentName.has_indexdataextentname())
    {
        hash_combine(hash, extentName.indexdataextentname().indexnumber());
        hash_combine(hash, extentName.indexdataextentname().partitionnumber());
    }
    if (extentName.has_indexheaderextentname())
    {
        hash_combine(hash, extentName.indexheaderextentname().indexnumber());
        hash_combine(hash, extentName.indexheaderextentname().partitionnumber());
    }
    if (extentName.has_logextentname())
    {
        hash_combine(hash, extentName.logextentname().logextentsequencenumber());
    }
    return hash;
}

namespace Phantom::ProtoStore
{

bool operator==(
    const ExtentName& left,
    const ExtentName& right
    ) noexcept
{
    return
        left.ExtentNameType_case() == right.ExtentNameType_case()
        && (
            left.has_databaseheaderextentname() ? (
                left.databaseheaderextentname().headercopynumber() == right.databaseheaderextentname().headercopynumber()
                )
            : left.has_indexdataextentname() ? (
                left.indexdataextentname().indexnumber() == right.indexdataextentname().indexnumber()
                && left.indexdataextentname().partitionnumber() == right.indexdataextentname().partitionnumber()
                && left.indexdataextentname().indexname() == right.indexdataextentname().indexname()
                )
            : left.has_indexheaderextentname() ? (
                left.indexheaderextentname().indexnumber() == right.indexheaderextentname().indexnumber()
                && left.indexheaderextentname().partitionnumber() == right.indexheaderextentname().partitionnumber()
                && left.indexheaderextentname().indexname() == right.indexheaderextentname().indexname()
                )
            : left.has_logextentname() ? (
                left.logextentname().logextentsequencenumber() == right.logextentname().logextentsequencenumber()
                )
            : true
            );
}

ExtentName MakeDatabaseHeaderExtentName(
    uint64_t copyNumber)
{
    ExtentName extentName;
    extentName.mutable_databaseheaderextentname()->set_headercopynumber(copyNumber);
    return extentName;
}

ExtentName MakeLogExtentName(
    uint64_t logExtentSequenceNumber)
{
    ExtentName extentName;
    extentName.mutable_logextentname()->set_logextentsequencenumber(logExtentSequenceNumber);
    return extentName;
}

ExtentName MakePartitionDataExtentName(
    ExtentName partitionHeaderExtentName)
{
    auto indexExtentName = move(*partitionHeaderExtentName.mutable_indexheaderextentname());
    *partitionHeaderExtentName.mutable_indexdataextentname() = move(indexExtentName);
    return partitionHeaderExtentName;
}

ExtentName MakePartitionHeaderExtentName(
    IndexNumber indexNumber,
    PartitionNumber partitionNumber,
    LevelNumber levelNumber,
    std::string indexName)
{
    ExtentName extentName;
    extentName.mutable_indexheaderextentname()->set_indexnumber(indexNumber);
    extentName.mutable_indexheaderextentname()->set_partitionnumber(partitionNumber);
    extentName.mutable_indexheaderextentname()->set_level(levelNumber);
    extentName.mutable_indexheaderextentname()->set_indexname(move(indexName));
    return extentName;
}

ExtentName MakeExtentName(
    const FlatBuffers::ExtentNameT& extentNameT)
{
    FlatMessage<FlatBuffers::ExtentName> extentName{ &extentNameT };
    return MakeExtentName(
        *extentName.get());
}

ExtentName MakeExtentName(
    const FlatBuffers::ExtentName& extentNameT
)
{
    ExtentName extentName;
    if (extentNameT.extent_name_as_DatabaseHeaderExtentName())
    {
        extentName.mutable_databaseheaderextentname()->set_headercopynumber(
            extentNameT.extent_name_as_DatabaseHeaderExtentName()->header_copy_number()
        );
    }
    if (extentNameT.extent_name_as_IndexDataExtentName())
    {
        extentName.mutable_indexdataextentname()->set_indexname(
            extentNameT.extent_name_as_IndexDataExtentName()->index_extent_name()->index_name()->str());
        extentName.mutable_indexdataextentname()->set_indexnumber(
            extentNameT.extent_name_as_IndexDataExtentName()->index_extent_name()->index_number());
        extentName.mutable_indexdataextentname()->set_level(
            extentNameT.extent_name_as_IndexDataExtentName()->index_extent_name()->level());
        extentName.mutable_indexdataextentname()->set_partitionnumber(
            extentNameT.extent_name_as_IndexDataExtentName()->index_extent_name()->partition_number());
    }
    if (extentNameT.extent_name_as_IndexHeaderExtentName())
    {
        extentName.mutable_indexheaderextentname()->set_indexname(
            extentNameT.extent_name_as_IndexHeaderExtentName()->index_extent_name()->index_name()->str());
        extentName.mutable_indexheaderextentname()->set_indexnumber(
            extentNameT.extent_name_as_IndexHeaderExtentName()->index_extent_name()->index_number());
        extentName.mutable_indexheaderextentname()->set_level(
            extentNameT.extent_name_as_IndexHeaderExtentName()->index_extent_name()->level());
        extentName.mutable_indexheaderextentname()->set_partitionnumber(
            extentNameT.extent_name_as_IndexHeaderExtentName()->index_extent_name()->partition_number());
    }
    if (extentNameT.extent_name_as_LogExtentName())
    {
        extentName.mutable_logextentname()->set_logextentsequencenumber(
            extentNameT.extent_name_as_LogExtentName()->log_extent_sequence_number());
    }
    return std::move(extentName);
}

FlatBuffers::ExtentNameT MakeExtentName(
    const ExtentName& extentName
)
{
    FlatBuffers::ExtentNameT extentNameT;
    FlatBuffers::ExtentNameUnionUnion extentNameTUnion;
    if (extentName.has_databaseheaderextentname())
    {
        FlatBuffers::DatabaseHeaderExtentNameT databaseHeaderExtentNameT;
        databaseHeaderExtentNameT.header_copy_number = extentName.databaseheaderextentname().headercopynumber();
        extentNameTUnion.Set(std::move(databaseHeaderExtentNameT));
    }
    if (extentName.has_indexdataextentname())
    {
        FlatBuffers::IndexDataExtentNameT indexDataExtentNameT;
        indexDataExtentNameT.index_extent_name = std::make_unique<FlatBuffers::IndexExtentNameT>();
        indexDataExtentNameT.index_extent_name->index_name = extentName.indexdataextentname().indexname();
        indexDataExtentNameT.index_extent_name->index_number = extentName.indexdataextentname().indexnumber();
        indexDataExtentNameT.index_extent_name->level = extentName.indexdataextentname().level();
        indexDataExtentNameT.index_extent_name->partition_number = extentName.indexdataextentname().partitionnumber();
        extentNameTUnion.Set(std::move(indexDataExtentNameT));
    }
    if (extentName.has_indexheaderextentname())
    {
        FlatBuffers::IndexHeaderExtentNameT indexHeaderExtentNameT;
        indexHeaderExtentNameT.index_extent_name = std::make_unique<FlatBuffers::IndexExtentNameT>();
        indexHeaderExtentNameT.index_extent_name->index_name = extentName.indexheaderextentname().indexname();
        indexHeaderExtentNameT.index_extent_name->index_number = extentName.indexheaderextentname().indexnumber();
        indexHeaderExtentNameT.index_extent_name->level = extentName.indexheaderextentname().level();
        indexHeaderExtentNameT.index_extent_name->partition_number = extentName.indexheaderextentname().partitionnumber();
        extentNameTUnion.Set(std::move(indexHeaderExtentNameT));
    }
    if (extentName.has_logextentname())
    {
        FlatBuffers::LogExtentNameT logExtentNameT;
        logExtentNameT.log_extent_sequence_number = extentName.logextentname().logextentsequencenumber();
        extentNameTUnion.Set(std::move(logExtentNameT));
    }
    extentNameT.extent_name = std::move(extentNameTUnion);
    return std::move(extentNameT);
}

flatbuffers::Offset<FlatBuffers::ExtentName> CreateExtentName(
    flatbuffers::FlatBufferBuilder& builder,
    const ExtentName& extentName
)
{
    auto extentNameT = MakeExtentName(
        extentName);

    return FlatBuffers::ExtentName::Pack(
        builder,
        &extentNameT);
}

}
