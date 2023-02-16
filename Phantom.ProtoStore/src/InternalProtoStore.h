#pragma once

#include "StandardTypes.h"
#include <cppcoro/async_mutex.hpp>

namespace Phantom::ProtoStore
{

class IInternalTransaction
    :
    public ICommittableTransaction,
    public SerializationTypes
{
public:
    virtual LogRecord& LogRecord(
    ) = 0;

    virtual LoggedUnresolvedTransactions& GetLoggedUnresolvedTransactions(
    ) = 0;
};

typedef std::function<status_task<>(IInternalTransaction*)> InternalTransactionVisitor;

class IInternalProtoStore
    :
    public IProtoStore,
    public SerializationTypes
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

    virtual shared_ptr<IIndex> GetPartitionsIndex(
    ) = 0;

    virtual shared_ptr<IIndex> GetMergeProgressIndex(
    ) = 0;

    virtual shared_ptr<IIndex> GetMergesIndex(
    ) = 0;

    virtual shared_ptr<IIndex> GetUnresolvedTransactionsIndex(
    ) = 0;

    virtual operation_task<TransactionSucceededResult> InternalExecuteTransaction(
        const BeginTransactionRequest beginRequest,
        InternalTransactionVisitor visitor
    ) = 0;

    virtual task<> LogCommitExtent(
        LogRecord& logRecord,
        ExtentName extentName
    ) = 0;

    virtual task<> LogDeleteExtentPendingPartitionsUpdated(
        LogRecord& logRecord,
        ExtentName extentName,
        CheckpointNumber partitionsTableCheckpointNumber
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
        LevelNumber levelNumber,
        ExtentName& out_headerExtentName,
        ExtentName& out_dataExtentName,
        shared_ptr<IPartitionWriter>& out_partitionWriter
    ) = 0;

};

}