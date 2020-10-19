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
#include "IndexDataSources.h"
#include "IndexPartitionMergeGenerator.h"
#include "InternalProtoStore.h"
#include "Phantom.System/single_pending_task.h"
#include "Phantom.System/encompassing_pending_task.h"
#include "IndexMerger.h"

namespace Phantom::ProtoStore
{

class ProtoStore
    :
    public IInternalProtoStore,
    public AsyncScopeMixin
{
    Schedulers m_schedulers;
    const shared_ptr<IExtentStore> m_headerExtentStore;
    const shared_ptr<IExtentStore> m_logExtentStore;
    const shared_ptr<IExtentStore> m_dataExtentStore;
    const shared_ptr<IExtentStore> m_dataHeaderExtentStore;
    const shared_ptr<IMessageStore> m_headerMessageStore;
    const shared_ptr<IMessageStore> m_logMessageStore;
    const shared_ptr<IMessageStore> m_dataMessageStore;
    const shared_ptr<IMessageStore> m_dataHeaderMessageStore;
    const shared_ptr<IRandomMessageAccessor> m_headerMessageAccessor;
    const shared_ptr<IRandomMessageAccessor> m_dataMessageAccessor;
    const shared_ptr<IRandomMessageAccessor> m_dataHeaderMessageAccessor;
    const shared_ptr<IHeaderAccessor> m_headerAccessor;
    
    std::set<IntegrityCheck> m_integrityChecks;

    std::optional<LogManager> m_logManager;
    
    Header m_header;

    uint64_t m_checkpointLogSize;

    cppcoro::inline_scheduler m_inlineScheduler;
    cppcoro::sequence_barrier<uint64_t> m_writeSequenceNumberBarrier;
    std::atomic<uint64_t> m_nextWriteSequenceNumber;
    std::atomic<IndexNumber> m_nextIndexNumber;
    std::atomic<PartitionNumber> m_nextPartitionNumber;

    cppcoro::async_mutex m_headerMutex;
    async_reader_writer_lock m_indexesByNumberLock;

    cppcoro::async_mutex m_updatePartitionsMutex;
    std::map<ExtentName, shared_ptr<IPartition>> m_activePartitions;
    vector<ExtentName> m_replayPartitionsActivePartitions;

    encompassing_pending_task m_checkpointTask;
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

    typedef unordered_map<google::protobuf::uint64, IndexEntry> IndexesByNumberMap;
    IndexesByNumberMap m_indexesByNumber;

    task<shared_ptr<IIndex>> GetIndexInternal(
        const string& indexName,
        SequenceNumber sequenceNumber
    );

    const bool DoReplayPartitions = true;
    const bool DontReplayPartitions = false;

    task<const IndexEntry&> GetIndexEntryInternal(
        google::protobuf::uint64 indexNumber,
        bool doReplayPartitions
    );

    virtual task<shared_ptr<IIndex>> GetIndex(
        google::protobuf::uint64 indexNumber)
        override;

    void MakeIndexesByNumberRow(
        IndexesByNumberKey& indexesByNumberKey,
        IndexesByNumberValue& indexesByNumberValue,
        const IndexName& indexName,
        IndexNumber indexNumber,
        SequenceNumber createSequenceNumber,
        const Descriptor* keyDescriptor,
        const Descriptor* valueDescriptor
    );

    IndexEntry MakeIndex(
        const IndexesByNumberKey& indexesKey,
        const IndexesByNumberValue& indexesValue
    );

    task<IndexNumber> AllocateIndexNumber();

    task<ExtentName> AllocateDataExtent();

    virtual task<> OpenPartitionWriter(
        ExtentName& out_dataExtentNumber,
        shared_ptr<IPartitionWriter>& out_partitionWriter
    ) override;

    task<> LogCommitDataExtent(
        LogRecord& logRecord,
        ExtentName extentName
    );

    task<> Replay(
        ExtentName logExtent);

    task<> Replay(
        const LogRecord& logRecord);

    task<> Replay(
        const LoggedUpdatePartitions& logRecord);

    task<> Replay(
        const LoggedAction& logRecord);

    task<> Replay(
        const LoggedCreateIndex& logRecord);

    task<> Replay(
        const LoggedCheckpoint& logRecord);

    task<> Replay(
        const LoggedCommitExtent& logRecord);

    task<> Replay(
        const LoggedCreateExtent& logRecord);

    task<> Replay(
        const LoggedDeleteExtent& logRecord);

    task<> Replay(
        const LoggedPartitionsData& logRecord);

    task<> ReplayPartitionsForOpenedIndexes();

    task<> ReplayPartitionsForIndex(
        const IndexEntry& indexEntry);

    task<> WriteLogRecord(
        const LogRecord& logRecord);

    task<> SwitchToNewLog();

    task<> UpdateHeader(
        std::function<task<>(Header&)> modifier
    );

    task<> Checkpoint(
        IndexEntry indexEntry
    );

    task<shared_ptr<IPartition>> OpenPartitionForIndex(
        const shared_ptr<IIndex>& index,
        ExtentName dataExtentNumber,
        ExtentName headerExtentNumber);

    virtual task<vector<std::tuple<PartitionsKey, PartitionsValue>>> GetPartitionsForIndex(
        IndexNumber indexNumber
    ) override;

    virtual task<vector<shared_ptr<IPartition>>> OpenPartitionsForIndex(
        const shared_ptr<IIndex>& index,
        const vector<ExtentName>& dataExtentNumbers
    ) override;

    shared_task<OperationResult> InternalExecuteOperation(
        InternalOperationVisitor visitor,
        uint64_t readSequenceNumber,
        uint64_t thisWriteSequenceNumber);

    task<OperationResult> InternalExecuteOperation(
        const BeginTransactionRequest beginRequest,
        InternalOperationVisitor visitor
    );

    task<> Publish(
        shared_task<OperationResult> operationResult,
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

    virtual task<shared_ptr<IIndex>> GetPartitionsIndex(
    ) override;

    virtual task<shared_ptr<IIndex>> GetMergeProgressIndex(
    ) override;

    virtual task<shared_ptr<IIndex>> GetMergesIndex(
    ) override;

    friend class Operation;

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
        Schedulers schedulers,
        shared_ptr<IExtentStore> headerStore,
        shared_ptr<IExtentStore> logStore,
        shared_ptr<IExtentStore> dataStore,
        shared_ptr<IExtentStore> dataHeaderStore);

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

    virtual task<OperationResult> ExecuteOperation(
        const BeginTransactionRequest beginRequest,
        OperationVisitor visitor
    ) override;

    virtual task<ProtoIndex> CreateIndex(
        const CreateIndexRequest& createIndexRequest
    ) override;

    virtual task<ProtoIndex> GetIndex(
        const GetIndexRequest& getIndexRequest
    ) override;

    virtual task<ReadResult> Read(
        const ReadRequest& readRequest
    ) override;

    virtual async_generator<EnumerateResult> Enumerate(
        const EnumerateRequest& enumerateRequest
    ) override;

    virtual task<> Join(
    ) override;
};
}