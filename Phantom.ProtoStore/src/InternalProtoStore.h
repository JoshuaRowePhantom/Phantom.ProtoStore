#pragma once

#include "StandardTypes.h"
#include <cppcoro/async_mutex.hpp>
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"
#include "LogExtentUsageMap.h"
#include "SequenceNumber.h"

namespace Phantom::ProtoStore
{

struct SystemIndexNumbers
{
    static constexpr IndexNumber IndexesByNumber = 1;
    static constexpr IndexNumber IndexesByName = 2;
    static constexpr IndexNumber Partitions = 3;
    static constexpr IndexNumber Merges = 4;
    static constexpr IndexNumber MergeProgress = 5;
    static constexpr IndexNumber DistributedTransactions = 6;
    static constexpr IndexNumber DistributedTransactionReferences = 7;
    static constexpr IndexNumber FirstUserIndexNumber = 1000;

    static constexpr bool IsSystemIndex(
        IndexNumber indexNumber
    )
    {
        return indexNumber < FirstUserIndexNumber;
    }
};

struct GlobalSequenceNumbers
{
    AtomicSequenceNumber<IndexNumber> m_nextIndexNumber = SystemIndexNumbers::FirstUserIndexNumber;
    AtomicSequenceNumber<PartitionNumber> m_nextPartitionNumber = 1;
    AtomicSequenceNumber<LocalTransactionNumber> m_nextLocalTransactionNumber = 1;
};

class IMemoryTable;

class IInternalTransaction
    :
    public ICommittableTransaction,
    public SerializationTypes
{
public:
    virtual void BuildLogRecord(
        LogEntryUnion logEntry,
        std::function<Offset<void>(flatbuffers::FlatBufferBuilder&)> builder
    ) = 0;

    template<
        IsNativeTable NativeTable
    > void BuildLogRecord(
        const NativeTable& nativeTable
    )
    {
        BuildLogRecord(
            FlatBuffers::LogEntryUnionUnionTraits<NativeTable>::enum_value,
            [&](auto& builder)
        {
            return NativeTable::TableType::Pack(
                builder,
                &nativeTable
            ).Union();
        });
    }

    virtual operation_task<FlatMessage<LoggedRowWrite>> AddRowInternal(
        const WriteOperationMetadata& writeOperationMetadata,
        ProtoIndex protoIndex,
        const ProtoValue& key,
        const ProtoValue& value
    ) = 0;

    virtual void BuildCommitPartitionLogEntries(
        const ExtentNameT& headerExtentName,
        const ExtentNameT& dataExtentName
    ) = 0;

    //virtual LoggedUnresolvedTransactions& GetLoggedUnresolvedTransactions(
    //) = 0;
};

typedef std::function<status_task<>(IInternalTransaction*)> InternalTransactionVisitor;

class IInternalProtoStoreTransactionFactory
{
public:
    virtual operation_task<TransactionSucceededResult> InternalExecuteTransaction(
        const BeginTransactionRequest beginRequest,
        InternalTransactionVisitor visitor
    ) = 0;
};

class IInternalProtoStore
    :
    public IProtoStore,
    public IInternalProtoStoreTransactionFactory,
    public SerializationTypes
{
public:
    virtual cppcoro::async_mutex_scoped_lock_operation AcquireUpdatePartitionsLock(
    ) = 0;

    virtual task<> UpdatePartitionsForIndex(
        IndexNumber indexNumber,
        cppcoro::async_mutex_lock& acquiredUpdatePartitionsLock
    ) = 0;

    virtual task<partition_row_list_type> GetPartitionsForIndex(
        IndexNumber indexNumber
    ) = 0;

    virtual shared_ptr<IIndex> GetPartitionsIndex(
    ) = 0;

    virtual shared_ptr<IIndex> GetMergeProgressIndex(
    ) = 0;

    virtual shared_ptr<IIndex> GetMergesIndex(
    ) = 0;

    virtual task<shared_ptr<IIndex>> GetIndex(
        google::protobuf::uint64 indexNumber
    ) = 0;

    virtual task<PartitionNumber> CreateMemoryTable(
        const std::shared_ptr<IIndex>& index,
        PartitionNumber partitionNumber,
        std::shared_ptr<IMemoryTable>& memoryTable
    ) = 0;

    virtual task<vector<shared_ptr<IPartition>>> OpenPartitionsForIndex(
        const shared_ptr<IIndex>& index,
        const vector<FlatValue<FlatBuffers::IndexHeaderExtentName>>& headerExtentNames
    ) = 0;

    virtual task<> OpenPartitionWriter(
        IndexNumber indexNumber,
        IndexName indexName,
        std::shared_ptr<const Schema> schema,
        std::shared_ptr<const ValueComparer> keyComparer,
        std::shared_ptr<const ValueComparer> valueComparer,
        LevelNumber levelNumber,
        FlatBuffers::ExtentNameT& out_headerExtentName,
        FlatBuffers::ExtentNameT& out_dataExtentName,
        shared_ptr<IPartitionWriter>& out_partitionWriter
    ) = 0;
};

struct IProtoStoreReplayTarget
{
public:
    virtual task<shared_ptr<IIndex>> GetIndex(
        google::protobuf::uint64 indexNumber
    ) = 0;

    virtual task<> ReplayPartitionsData(
        const FlatMessage<FlatBuffers::LoggedPartitionsData>& loggedPartitionsData
    ) = 0;

    virtual task<std::shared_ptr<IMemoryTable>> ReplayCreateMemoryTable(
        IndexNumber indexNumber,
        PartitionNumber partitionNumber
    ) = 0;

    virtual task<> ReplayPartitionsForOpenedIndexes(
    ) = 0;

    virtual task<> ReplayGlobalSequenceNumbers(
        GlobalSequenceNumbers globalSequenceNumbers
    ) = 0;

    virtual task<> ReplayLogExtentUsageMap(
        LogExtentUsageMap logExtentUsageMap
    ) = 0;
};

}