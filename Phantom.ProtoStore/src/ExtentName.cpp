#include "StandardTypes.h"
#include "ExtentName.h"
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
    std::string indexName)
{
    ExtentName extentName;
    extentName.mutable_indexheaderextentname()->set_indexnumber(indexNumber);
    extentName.mutable_indexheaderextentname()->set_partitionnumber(partitionNumber);
    extentName.mutable_indexheaderextentname()->set_indexname(move(indexName));
    return extentName;
}

}
