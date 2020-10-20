#pragma once

#include "StandardTypes.h"
#include <cppcoro/async_mutex.hpp>

namespace Phantom::ProtoStore
{

class IInternalOperation
    :
    public IOperationTransaction
{
public:
    virtual LogRecord& LogRecord(
    ) = 0;
};

typedef std::function<task<>(IInternalOperation*)> InternalOperationVisitor;

class IInternalProtoStore
    :
    public IProtoStore
{
public:
    virtual cppcoro::async_mutex_scoped_lock_operation AcquireUpdatePartitionsLock(
    ) = 0;

    virtual task<> UpdatePartitionsForIndex(
        IndexNumber indexNumber,
        cppcoro::async_mutex_lock& acquiredUpdatePartitionsLock
    ) = 0;

    virtual task<vector<std::tuple<PartitionsKey, PartitionsValue>>> GetPartitionsForIndex(
        IndexNumber indexNumber
    ) = 0;

    virtual task<shared_ptr<IIndex>> GetPartitionsIndex(
    ) = 0;

    virtual task<shared_ptr<IIndex>> GetMergeProgressIndex(
    ) = 0;

    virtual task<shared_ptr<IIndex>> GetMergesIndex(
    ) = 0;

    virtual task<OperationResult> InternalExecuteOperation(
        const BeginTransactionRequest beginRequest,
        InternalOperationVisitor visitor
    ) = 0;

    virtual task<> LogCommitDataExtent(
        LogRecord& logRecord,
        ExtentName extentName
    ) = 0;

    virtual task<shared_ptr<IIndex>> GetIndex(
        google::protobuf::uint64 indexNumber
    ) = 0;

    virtual task<vector<shared_ptr<IPartition>>> OpenPartitionsForIndex(
        const shared_ptr<IIndex>& index,
        const vector<ExtentName>& headerExtentNames
    ) = 0;

    virtual task<> OpenPartitionWriter(
        IndexNumber indexNumber,
        IndexName indexName,
        ExtentName& out_headerExtentName,
        ExtentName& out_dataExtentName,
        shared_ptr<IPartitionWriter>& out_partitionWriter
    ) = 0;

};

}