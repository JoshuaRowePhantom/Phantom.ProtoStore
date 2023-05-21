#pragma once

#include <Phantom.ProtoStore/Phantom.ProtoStore.h>
#include "StandardTypes.h"
#include "Phantom.System/async_reader_writer_lock.h"
#include <cppcoro/async_mutex.hpp>
#include <cppcoro/async_scope.hpp>
#include <cppcoro/inline_scheduler.hpp>
#include <cppcoro/sequence_barrier.hpp>
#include <cppcoro/shared_task.hpp>
#include "AsyncScopeMixin.h"
#include "LogManager.h"
#include "HeaderAccessor.h"
#include "IndexDataSources.h"
#include "IndexPartitionMergeGenerator.h"
#include "InternalProtoStore.h"
#include "Phantom.System/single_pending_task.h"
#include "Phantom.System/encompassing_pending_task.h"
#include "IndexMerger.h"
#include "ExtentName.h"
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"

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
};

class ProtoStore
    :
    public IInternalProtoStore,
    public AsyncScopeMixin
{
    Schedulers m_schedulers;
    const shared_ptr<IExtentStore> m_extentStore;
    const shared_ptr<IMessageStore> m_messageStore;
    const shared_ptr<IHeaderAccessor> m_headerAccessor;
    const MergeParameters m_defaultMergeParameters;

    std::set<IntegrityCheck> m_integrityChecks;

    std::optional<LogManager> m_logManager;
    
    std::unique_ptr<FlatBuffers::DatabaseHeaderT> m_header;

    uint64_t m_checkpointLogSize;

    cppcoro::inline_scheduler m_inlineScheduler;
    cppcoro::sequence_barrier<uint64_t> m_writeSequenceNumberBarrier;
    std::atomic<uint64_t> m_nextWriteSequenceNumber;
    std::atomic<IndexNumber> m_nextIndexNumber;
    std::atomic<PartitionNumber> m_nextPartitionNumber;
    std::atomic<MemoryTableTransactionSequenceNumber> m_memoryTableTransactionSequenceNumber = 1;
    std::atomic<LocalTransactionNumber> m_localTransactionNumber = 1;

    cppcoro::async_mutex m_headerMutex;
    async_reader_writer_lock m_indexesByNumberLock;

    cppcoro::async_mutex m_updatePartitionsMutex;
    std::unordered_map<
        FlatValue<FlatBuffers::IndexHeaderExtentName>, 
        shared_ptr<IPartition>,
        ProtoValueStlHash,
        ProtoValueStlEqual
    > m_activePartitions;
    vector<ExtentName> m_replayPartitionsActivePartitions;

    encompassing_pending_task<> m_encompassingCheckpointTask;
    single_pending_task m_mergeTask;

    std::atomic<ExtentOffset> m_nextCheckpointLogOffset;

    struct IndexEntry
    {
        IndexNumber IndexNumber;
        shared_ptr<IIndexDataSources> DataSources;
        shared_ptr<IIndex> Index;
        shared_ptr<IndexPartitionMergeGenerator> MergeGenerator;
    };

    IndexEntry m_indexesByNumberIndex;
    IndexEntry m_indexesByNameIndex;
    IndexEntry m_partitionsIndex;
    IndexEntry m_mergesIndex;
    IndexEntry m_mergeProgressIndex;
    IndexEntry m_distributedTransactionsIndex;
    IndexEntry m_distributedTransactionReferencesIndex;

    shared_ptr<IUnresolvedTransactionsTracker> m_unresolvedTransactionsTracker;

    typedef unordered_map<google::protobuf::uint64, IndexEntry> IndexesByNumberMap;
    IndexesByNumberMap m_indexesByNumber;

    task<shared_ptr<IIndex>> GetIndexInternal(
        string indexName,
        SequenceNumber sequenceNumber
    );

    std::unordered_map<LocalTransactionNumber, std::unordered_map<uint32_t, FlatMessage<LoggedRowWrite>>> m_replayedWrites;

    const bool DoReplayPartitions = true;
    const bool DontReplayPartitions = false;

    task<const IndexEntry*> GetIndexEntryInternal(
        google::protobuf::uint64 indexNumber,
        bool doReplayPartitions
    );

    virtual task<shared_ptr<IIndex>> GetIndex(
        google::protobuf::uint64 indexNumber)
        override;

    void MakeIndexesByNumberRow(
        FlatValue<IndexesByNumberKey>& indexesByNumberKey,
        FlatValue<IndexesByNumberValue>& indexesByNumberValue,
        const IndexName& indexName,
        IndexNumber indexNumber,
        SequenceNumber createSequenceNumber,
        const Schema& schema
    );

    IndexEntry MakeIndex(
        FlatValue<IndexesByNumberKey> indexesByNumberKey,
        FlatValue<IndexesByNumberValue> indexesByNumberValue
    );

    task<IndexNumber> AllocateIndexNumber();

    task<> AllocatePartitionExtents(
        IndexNumber indexNumber,
        IndexName indexName,
        LevelNumber levelNumber,
        ExtentNameT& out_partitionHeaderExtentName,
        ExtentNameT& out_partitionDataExtentName);

    virtual task<PartitionNumber> CreateMemoryTable(
        const std::shared_ptr<IIndex>& index,
        PartitionNumber partitionNumber,
        std::shared_ptr<IMemoryTable>& memoryTable
    ) override;

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
    ) override;

    bool IsLogEntryForPhase(
        int phase,
        const FlatMessage<LogEntry>& logEntry
    );

    task<> Replay();

    task<> Replay(
        int phase,
        const ExtentName* extentName);

    task<> Replay(
        const FlatMessage<LogEntry>& logEntry);

    task<> Replay(
        const FlatMessage<LoggedRowWrite>& logRecord);

    task<> Replay(
        const FlatMessage<LoggedCommitLocalTransaction>& logRecord);

    task<> Replay(
        const FlatMessage<LoggedUpdatePartitions>& logRecord);

    task<> Replay(
        const LoggedAction& logRecord);

    task<> Replay(
        const FlatMessage<LoggedCreateIndex>& logRecord);

    task<> Replay(
        const FlatMessage<LoggedCreatePartition>& logRecord);

    task<> Replay(
        const FlatMessage<LoggedPartitionsData>& logRecord);

    task<> ReplayPartitionsForOpenedIndexes();
    task<> CreateInitialMemoryTables();

    task<> ReplayPartitionsForIndex(
        const IndexEntry& indexEntry);

    task<> SwitchToNewLog();

    task<> UpdateHeader(
        std::function<task<>(FlatBuffers::DatabaseHeaderT*)> modifier
    );

    task<> Checkpoint(
        IndexEntry indexEntry
    );

    task<shared_ptr<IPartition>> OpenPartitionForIndex(
        const shared_ptr<IIndex>& index,
        const FlatBuffers::IndexHeaderExtentName* headerExtentName);

    virtual task<partition_row_list_type> GetPartitionsForIndex(
        IndexNumber indexNumber
    ) override;

    virtual task<vector<shared_ptr<IPartition>>> OpenPartitionsForIndex(
        const shared_ptr<IIndex>& index,
        const vector<FlatValue<FlatBuffers::IndexHeaderExtentName>>& headerExtentNames
    ) override;

    shared_task<TransactionResult> InternalExecuteTransaction(
        InternalTransactionVisitor visitor,
        uint64_t readSequenceNumber,
        uint64_t thisWriteSequenceNumber);

    operation_task<TransactionSucceededResult> InternalExecuteTransaction(
        const BeginTransactionRequest beginRequest,
        InternalTransactionVisitor visitor
    );

    task<> Publish(
        shared_task<TransactionResult> transactionResult,
        uint64_t previousWriteSequenceNumber,
        uint64_t thisWriteSequenceNumber);

    task<> InternalCheckpoint();
    task<> InternalMerge();

    virtual cppcoro::async_mutex_scoped_lock_operation AcquireUpdatePartitionsLock(
    ) override;

    virtual task<> UpdatePartitionsForIndex(
        IndexNumber indexNumber,
        cppcoro::async_mutex_lock& acquiredUpdatePartitionsLock
    ) override;

    virtual shared_ptr<IIndex> GetPartitionsIndex(
    ) override;

    virtual shared_ptr<IIndex> GetMergeProgressIndex(
    ) override;

    virtual shared_ptr<IIndex> GetMergesIndex(
    ) override;

    friend class LocalTransaction;
    friend class TestAccessors;

public:

    // Noncopyable, nonmovable.
    ProtoStore(
        const ProtoStore&
    ) = delete;

    ProtoStore(
        ProtoStore&&
    ) = delete;

    ProtoStore& operator=(
        const ProtoStore&
        ) = delete;

    ProtoStore& operator=(
        ProtoStore&&
        ) = delete;

    ProtoStore(
        const OpenProtoStoreRequest& openRequest,
        shared_ptr<IExtentStore> extentStore);

    ~ProtoStore();

    task<> Open(
        const OpenProtoStoreRequest& openRequest
    );

    task<> Create(
        const CreateProtoStoreRequest& openRequest
    );

    virtual task<> Checkpoint(
    ) override;

    virtual task<> Merge(
    ) override;

    virtual operation_task<TransactionSucceededResult> ExecuteTransaction(
        const BeginTransactionRequest beginRequest,
        TransactionVisitor visitor
    ) override;

    virtual operation_task<ProtoIndex> CreateIndex(
        const CreateIndexRequest& createIndexRequest
    ) override;

    virtual operation_task<ProtoIndex> GetIndex(
        const GetIndexRequest& getIndexRequest
    ) override;

    virtual operation_task<ReadResult> Read(
        const ReadRequest& readRequest
    ) override;

    virtual async_generator<OperationResult<EnumerateResult>> Enumerate(
        EnumerateRequest enumerateRequest
    ) override;
    
    virtual async_generator<OperationResult<EnumerateResult>> EnumeratePrefix(
        EnumeratePrefixRequest enumerateRequest
    ) override;

    virtual task<> Join(
    ) override;
};
}