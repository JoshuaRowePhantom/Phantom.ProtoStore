#include "ExtentName.h"
#include "StandardTypes.h"
#include "ProtoStore.h"
#include "MessageStore.h"
#include "RandomMessageAccessor.h"
#include "HeaderAccessor.h"
#include "Schema.h"
#include "ProtoStoreInternal.pb.h"
#include "IndexImpl.h"
#include "IndexDataSourcesImpl.h"
#include "MemoryTableImpl.h"
#include "Phantom.System/async_value_source.h"
#include "PartitionImpl.h"
#include "PartitionWriterImpl.h"
#include <cppcoro/sync_wait.hpp>
#include "IndexMerger.h"
#include "IndexPartitionMergeGenerator.h"
#include "UnresolvedTransactionsTracker.h"
#include <algorithm>

namespace Phantom::ProtoStore
{

ProtoStore::ProtoStore(
    const OpenProtoStoreRequest& openRequest,
    shared_ptr<IExtentStore> extentStore
)
    :
    m_schedulers(openRequest.Schedulers),
    m_extentStore(move(extentStore)),
    m_defaultMergeParameters(openRequest.DefaultMergeParameters),
    m_messageStore(MakeMessageStore(m_schedulers, m_extentStore)),
    m_messageAccessor(MakeRandomMessageAccessor(m_messageStore)),
    m_headerAccessor(MakeHeaderAccessor(m_messageAccessor)),
    m_encompassingCheckpointTask(),
    m_mergeTask([=] { return InternalMerge(); })
{
    m_writeSequenceNumberBarrier.publish(0);
}


ProtoStore::~ProtoStore()
{
    SyncDestroy();
}

task<> ProtoStore::Create(
    const CreateProtoStoreRequest& createRequest)
{
    m_schedulers = createRequest.Schedulers;

    auto header = std::make_unique<FlatBuffers::DatabaseHeaderT>();
    header->version = 1,
    header->log_alignment = createRequest.LogAlignment,
    header->epoch = 0,
    header->next_index_number = 1000,
    header->next_partition_number = 0,

    // Write both copies of the header,
    // so that ordinary open operations don't get a range_error exception
    // when reopening.
    co_await m_headerAccessor->WriteHeader(
        header.get());
    co_await m_headerAccessor->WriteHeader(
        header.get());

    co_await Open(
        createRequest);
}

task<> ProtoStore::Open(
    const OpenProtoStoreRequest& openRequest)
{
    m_integrityChecks = openRequest.IntegrityChecks;
    m_schedulers = openRequest.Schedulers;
    m_checkpointLogSize = openRequest.CheckpointLogSize;
    m_nextCheckpointLogOffset.store(m_checkpointLogSize);

    m_header = co_await m_headerAccessor->ReadHeader();

    m_nextPartitionNumber.store(
        m_header->next_partition_number);
    m_nextIndexNumber.store(
        m_header->next_index_number);

    {
        IndexesByNumberKey indexesByNumberKey;
        IndexesByNumberValue indexesByNumberValue;

        MakeIndexesByNumberRow(
            indexesByNumberKey,
            indexesByNumberValue,
            "__System.IndexesByNumber",
            1,
            SequenceNumber::Earliest,
            IndexesByNumberKey::descriptor(),
            IndexesByNumberValue::descriptor());

        m_indexesByNumberIndex = MakeIndex(
            indexesByNumberKey,
            indexesByNumberValue
        );
    }

    {
        IndexesByNumberKey indexesByNumberKey;
        IndexesByNumberValue indexesByNumberValue;

        MakeIndexesByNumberRow(
            indexesByNumberKey,
            indexesByNumberValue,
            "__System.IndexesByName",
            2,
            SequenceNumber::Earliest,
            IndexesByNameKey::descriptor(),
            IndexesByNameValue::descriptor());

        m_indexesByNameIndex = MakeIndex(
            indexesByNumberKey,
            indexesByNumberValue
        );
    }

    {
        IndexesByNumberKey indexesByNumberKey;
        IndexesByNumberValue indexesByNumberValue;

        MakeIndexesByNumberRow(
            indexesByNumberKey,
            indexesByNumberValue,
            "__System.Partitions",
            3,
            SequenceNumber::Earliest,
            PartitionsKey::descriptor(),
            PartitionsValue::descriptor());

        m_partitionsIndex = MakeIndex(
            indexesByNumberKey,
            indexesByNumberValue
        );
    }

    {
        IndexesByNumberKey indexesByNumberKey;
        IndexesByNumberValue indexesByNumberValue;

        MakeIndexesByNumberRow(
            indexesByNumberKey,
            indexesByNumberValue,
            "__System.Merges",
            4,
            SequenceNumber::Earliest,
            MergesKey::descriptor(),
            MergesValue::descriptor());

        m_mergesIndex = MakeIndex(
            indexesByNumberKey,
            indexesByNumberValue
        );
    }

    {
        IndexesByNumberKey indexesByNumberKey;
        IndexesByNumberValue indexesByNumberValue;

        MakeIndexesByNumberRow(
            indexesByNumberKey,
            indexesByNumberValue,
            "__System.MergeProgress",
            5,
            SequenceNumber::Earliest,
            MergeProgressKey::descriptor(),
            MergeProgressValue::descriptor());

        m_mergeProgressIndex = MakeIndex(
            indexesByNumberKey,
            indexesByNumberValue
        );
    }

    {
        IndexesByNumberKey indexesByNumberKey;
        IndexesByNumberValue indexesByNumberValue;

        MakeIndexesByNumberRow(
            indexesByNumberKey,
            indexesByNumberValue,
            "__System.UnresolvedTransactions",
            6,
            SequenceNumber::Earliest,
            UnresolvedTransactionKey::descriptor(),
            UnresolvedTransactionValue::descriptor());

        m_unresolvedTransactionIndex = MakeIndex(
            indexesByNumberKey,
            indexesByNumberValue
        );
    }

    m_indexesByNumber[m_indexesByNumberIndex.IndexNumber] = m_indexesByNumberIndex;
    m_indexesByNumber[m_indexesByNameIndex.IndexNumber] = m_indexesByNameIndex;
    m_indexesByNumber[m_partitionsIndex.IndexNumber] = m_partitionsIndex;
    m_indexesByNumber[m_mergesIndex.IndexNumber] = m_mergesIndex;
    m_indexesByNumber[m_mergeProgressIndex.IndexNumber] = m_mergeProgressIndex;
    m_indexesByNumber[m_unresolvedTransactionIndex.IndexNumber] = m_unresolvedTransactionIndex;

    m_unresolvedTransactionsTracker = MakeUnresolvedTransactionsTracker(
        this);

    m_logManager.emplace(
        m_schedulers,
        m_extentStore,
        m_messageStore,
        m_header.get());

    // The first log record for the partitions index will be the actual
    // partitions to use, but the very first time a store is opened
    // it will have no data sources.
    co_await m_partitionsIndex.DataSources->UpdatePartitions(
        {},
        {});

    co_await ReplayPartitionsForOpenedIndexes();

    for (auto& logReplayExtentName : m_header->log_replay_extent_names)
    {
        co_await Replay(
            MakeLogExtentName(logReplayExtentName->log_extent_sequence_number),
            logReplayExtentName.get());
    }

    auto postUpdateHeaderTask = co_await m_logManager->FinishReplay(
        m_header.get());

    co_await UpdateHeader([](auto) -> task<> { co_return; });

    co_await postUpdateHeaderTask;
}

task<> ProtoStore::ReplayPartitionsForOpenedIndexes()
{
    for (auto indexEntry : m_indexesByNumber)
    {
        co_await ReplayPartitionsForIndex(
            indexEntry.second);
    }
}

task<> ProtoStore::ReplayPartitionsForIndex(
    const IndexEntry& indexEntry)
{
    auto partitionKeyValues = co_await GetPartitionsForIndex(
        indexEntry.IndexNumber);

    vector<ExtentName> headerExtentNames;

    for (auto& partitionKeyValue : partitionKeyValues)
    {
        headerExtentNames.push_back(
            get<PartitionsKey>(partitionKeyValue).headerextentname());
    }

    auto partitions = co_await OpenPartitionsForIndex(
        indexEntry.Index,
        headerExtentNames);

    co_await indexEntry.DataSources->UpdatePartitions(
        LoggedCheckpoint(),
        partitions);
}

task<> ProtoStore::UpdateHeader(
    std::function<task<>(FlatBuffers::DatabaseHeaderT*)> modifier
)
{
    auto lock = co_await m_headerMutex.scoped_lock_async();
        
    auto nextEpoch = m_header->epoch + 1;
    
    co_await modifier(
        m_header.get());

    m_header->epoch = nextEpoch;
    m_header->next_index_number = m_nextIndexNumber.load();
    m_header->next_partition_number = m_nextPartitionNumber.load();

    co_await m_headerAccessor->WriteHeader(
        m_header.get());
}

task<> ProtoStore::WriteLogRecord(
    const LogRecord& logRecord
)
{
    auto writeMessageResult = co_await m_logManager->WriteLogRecord(
        logRecord
    );

    auto nextCheckpointLogOffset = m_nextCheckpointLogOffset.load(
        std::memory_order_relaxed);

    if (writeMessageResult->DataRange.End == 0)
    {
        m_nextCheckpointLogOffset.store(
            m_checkpointLogSize);
    }
    else if (writeMessageResult->DataRange.End > nextCheckpointLogOffset
        && m_nextCheckpointLogOffset.compare_exchange_weak(
            nextCheckpointLogOffset,
            writeMessageResult->DataRange.End + m_checkpointLogSize))
    {
        spawn(
            Checkpoint());
    }
}

task<> ProtoStore::Replay(
    const ExtentName& logExtent,
    const LogExtentNameT* extentName
)
{
    auto logReader = co_await m_messageStore->OpenExtentForSequentialReadAccess(
        logExtent
    );

    while (true)
    {
        try
        {
            LogRecord logRecord;
            co_await logReader->Read(
                logRecord);

            co_await m_logManager->Replay(
                extentName,
                logRecord
            );

            co_await Replay(
                logRecord);
        }
        catch (std::range_error)
        {
            co_return;
        }
    }
}

task<> ProtoStore::Replay(
    const LogRecord& logRecord)
{
    for (auto& loggedRowWrite : logRecord.rows())
    {
        auto indexEntry = co_await GetIndexEntryInternal(
            loggedRowWrite.indexnumber(),
            DoReplayPartitions);

        co_await indexEntry->DataSources->Replay(
            loggedRowWrite);
    }

    for (auto& loggedAction : logRecord.extras().loggedactions())
    {
        co_await Replay(
            loggedAction);
    }
}

task<> ProtoStore::Replay(
    const LoggedAction& loggedAction
)
{
    if (loggedAction.has_loggedcreateindex())
    {
        co_await Replay(
            loggedAction.loggedcreateindex()
        );
    }

    if (loggedAction.has_loggedcheckpoints())
    {
        co_await Replay(
            loggedAction.loggedcheckpoints());
    }

    if (loggedAction.has_loggedpartitionsdata())
    {
        co_await Replay(
            loggedAction.loggedpartitionsdata());
    }
}

task<> ProtoStore::Replay(
    const LoggedCreateIndex& logRecord)
{
    if (m_nextIndexNumber.load() <= logRecord.indexnumber())
    {
        m_nextIndexNumber.store(logRecord.indexnumber() + 1);
    }
    co_return;
}

task<> ProtoStore::Replay(
    const LoggedPartitionsData& logRecord)
{
    auto replayPartitionsActivePartitions = vector(
        logRecord.headerextentnames().begin(),
        logRecord.headerextentnames().end());

    auto partitions = co_await OpenPartitionsForIndex(
        m_partitionsIndex.Index,
        replayPartitionsActivePartitions);

    co_await m_partitionsIndex.DataSources->UpdatePartitions(
        LoggedCheckpoint(),
        partitions);

    // If DataSources changed its partitions list, 
    // then all other indexes may have changed.
    co_await ReplayPartitionsForOpenedIndexes();
}

task<> ProtoStore::Replay(
    const LoggedCheckpoint& logRecord)
{
    co_return;
}

task<> ProtoStore::Replay(
    const LoggedCreatePartition& logRecord)
{
    if (m_nextPartitionNumber.load() <= logRecord.partitionnumber())
    {
        m_nextPartitionNumber.store(
            logRecord.partitionnumber() + 1);
    }
    co_return;
}

task<> ProtoStore::Replay(
    const LoggedUpdatePartitions& logRecord)
{
    if (m_indexesByNumber.contains(logRecord.indexnumber()))
    {
        co_await ReplayPartitionsForIndex(
            m_indexesByNumber[logRecord.indexnumber()]
        );
    }
}

task<ProtoIndex> ProtoStore::GetIndex(
    const GetIndexRequest& getIndexRequest
)
{
    auto index = co_await GetIndexInternal(
        getIndexRequest.IndexName,
        getIndexRequest.SequenceNumber
    );

    co_return ProtoIndex(
        index
    );
}

task<shared_ptr<IIndex>> ProtoStore::GetIndex(
    IndexNumber indexNumber)
{
    co_return (co_await GetIndexEntryInternal(indexNumber, DoReplayPartitions))->Index;
}

operation_task<ReadResult> ProtoStore::Read(
    const ReadRequest& readRequest
)
{
    return readRequest.Index.m_index->Read(
        nullptr,
        readRequest);
}

async_generator<OperationResult<EnumerateResult>> ProtoStore::Enumerate(
    const EnumerateRequest& enumerateRequest
)
{
    return enumerateRequest.Index.m_index->Enumerate(
        nullptr,
        enumerateRequest);
}

class LocalTransaction
    :
    public IInternalTransaction
{
    ProtoStore& m_protoStore;
    Serialization::LogRecord m_logRecord;
    shared_ptr<DelayedMemoryTableTransactionOutcome> m_delayedOperationOutcome;
    SequenceNumber m_readSequenceNumber;
    SequenceNumber m_initialWriteSequenceNumber;
    shared_task<CommitResult> m_commitTask;
    std::vector<FailedResult> m_failures;

    LoggedUnresolvedTransactions* m_loggedUnresolvedTransactions = nullptr;
    LoggedUnresolvedTransactions& GetLoggedUnresolvedTransactions()
    {
        if (!m_loggedUnresolvedTransactions)
        {
            m_loggedUnresolvedTransactions = m_logRecord.mutable_extras()->add_loggedactions()->mutable_loggedunresolvedtransactions();
        }
        return *m_loggedUnresolvedTransactions;
    }

public:
    LocalTransaction(
        MemoryTableTransactionSequenceNumber transactionSequenceNumber,
        ProtoStore& protoStore,
        SequenceNumber readSequenceNumber,
        SequenceNumber initialWriteSequenceNumber
    )
    :
        m_protoStore(protoStore),
        m_readSequenceNumber(readSequenceNumber),
        m_initialWriteSequenceNumber(initialWriteSequenceNumber),
        m_delayedOperationOutcome(std::make_shared<DelayedMemoryTableTransactionOutcome>(
            transactionSequenceNumber
            ))
    {
        m_commitTask = DelayedCommit();
    }

    ~LocalTransaction()
    {
        if (m_delayedOperationOutcome)
        {
            m_delayedOperationOutcome->Complete();
        }
    }

    Serialization::LogRecord& LogRecord()
    {
        return m_logRecord;
    }

    std::vector<FailedResult>& Failures()
    {
        return m_failures;
    }

    // Inherited via ITransaction
    virtual operation_task<> AddLoggedAction(
        const WriteOperationMetadata& writeOperationMetadata, 
        const Message* loggedAction, 
        LoggedOperationDisposition disposition
    ) override
    {
        return operation_task<>();
    }

    virtual operation_task<> AddRow(
        const WriteOperationMetadata& writeOperationMetadata, 
        ProtoIndex protoIndex,
        const ProtoValue& key,
        const ProtoValue& value
    ) override
    {
        auto index = protoIndex.m_index;

        auto readSequenceNumber = writeOperationMetadata.ReadSequenceNumber.value_or(
            m_readSequenceNumber);
        auto writeSequenceNumber = writeOperationMetadata.WriteSequenceNumber.value_or(
            m_initialWriteSequenceNumber);

        auto checkpointNumber = co_await index->AddRow(
            readSequenceNumber,
            key,
            value,
            writeSequenceNumber,
            writeOperationMetadata.TransactionId,
            m_delayedOperationOutcome
        );
        if (!checkpointNumber)
        {
            m_failures.push_back(checkpointNumber.error());
            co_return std::unexpected{ checkpointNumber.error() };
        }

        auto loggedRowWrite = m_logRecord.add_rows();
        if (writeOperationMetadata.WriteSequenceNumber.has_value())
        {
            loggedRowWrite->set_sequencenumber(ToUint64(*writeOperationMetadata.WriteSequenceNumber));
        }
        loggedRowWrite->set_indexnumber(protoIndex.m_index->GetIndexNumber());
        key.pack(
            loggedRowWrite->mutable_key());
        value.pack(
            loggedRowWrite->mutable_value());
        loggedRowWrite->set_checkpointnumber(
            *checkpointNumber);

        if (writeOperationMetadata.TransactionId)
        {
            co_await m_protoStore.m_unresolvedTransactionsTracker->LogUnresolvedTransaction(
                this,
                *loggedRowWrite
            );
        }

        co_return{};
    }

    virtual operation_task<> ResolveTransaction(
        const WriteOperationMetadata& writeOperationMetadata, 
        TransactionOutcome outcome
    ) override
    {
        return operation_task<>();
    }

    virtual task<ProtoIndex> GetIndex(
        const GetIndexRequest& getIndexRequest
    ) override
    {
        return m_protoStore.GetIndex(
            getIndexRequest);
    }

    virtual operation_task<ReadResult> Read(
        const ReadRequest& readRequest
    ) override
    {
        return m_protoStore.Read(
            readRequest);
    }

    virtual async_generator<OperationResult<EnumerateResult>> Enumerate(
        const EnumerateRequest& enumerateRequest
    ) override
    {
        return m_protoStore.Enumerate(
            enumerateRequest);
    }

    // Inherited via ICommittableTransaction
    virtual operation_task<CommitResult> Commit(
    ) override
    {
        co_return co_await m_commitTask;
    }

private:
    shared_task<CommitResult> DelayedCommit()
    {
        auto result = m_delayedOperationOutcome->BeginCommit(
            m_initialWriteSequenceNumber);

        if (result.Outcome == TransactionOutcome::Committed)
        {
            for (auto& addedRow : *m_logRecord.mutable_rows())
            {
                if (!addedRow.sequencenumber())
                {
                    addedRow.set_sequencenumber(
                        ToUint64(result.WriteSequenceNumber));
                }
            }

            co_await m_protoStore.WriteLogRecord(
                m_logRecord);
        }

        m_delayedOperationOutcome->Complete();

        co_return CommitResult
        {
            .Outcome = result.Outcome,
        };
    }
};

operation_task<TransactionSucceededResult> ProtoStore::ExecuteTransaction(
    const BeginTransactionRequest beginRequest,
    TransactionVisitor visitor
)
{
    co_return co_await InternalExecuteTransaction(
        beginRequest,
        [&](auto operation)
    {
        return visitor(operation);
    });
}

operation_task<TransactionSucceededResult> ProtoStore::InternalExecuteTransaction(
    const BeginTransactionRequest beginRequest,
    InternalTransactionVisitor visitor
)
{
    auto previousWriteSequenceNumber = m_nextWriteSequenceNumber.fetch_add(
        1,
        std::memory_order_acq_rel);
    auto thisWriteSequenceNumber = previousWriteSequenceNumber + 1;

    auto executionTransactionTask = InternalExecuteTransaction(
        visitor,
        thisWriteSequenceNumber,
        thisWriteSequenceNumber);

    //auto publishTask = Publish(
    //    executionOperationTask,
    //    previousWriteSequenceNumber,
    //    thisWriteSequenceNumber);

    //m_asyncScope.spawn(move(
    //    publishTask));

    co_return co_await executionTransactionTask;
}

shared_task<OperationResult<TransactionSucceededResult>> ProtoStore::InternalExecuteTransaction(
    InternalTransactionVisitor visitor,
    uint64_t readSequenceNumber,
    uint64_t thisWriteSequenceNumber
)
{
    LocalTransaction transaction(
        m_memoryTableTransactionSequenceNumber.fetch_add(1, std::memory_order_relaxed),
        *this,
        ToSequenceNumber(readSequenceNumber),
        ToSequenceNumber(thisWriteSequenceNumber));
    
    auto result = co_await visitor(&transaction);

    TransactionOutcome outcome = TransactionOutcome::Committed;
    if (result)
    {
        auto commitResult = co_await transaction.Commit();
        if (!commitResult)
        {
            outcome = TransactionOutcome::Aborted;
        }
        else
        {
            outcome = commitResult->Outcome;
        }
    }
    else
    {
        outcome = TransactionOutcome::Aborted;
    }

    if (outcome == TransactionOutcome::Committed)
    {
        co_return TransactionSucceededResult
        {
            .m_transactionOutcome = outcome,
        };
    }
    else
    {
        co_return std::unexpected
        {
            FailedResult
            {
                .ErrorCode = make_error_code(ProtoStoreErrorCode::AbortedTransaction),
                .ErrorDetails = TransactionFailedResult
                {
                    .TransactionOutcome = outcome,
                    .Failures = std::move(transaction.Failures()),
                },
            },
        };
    }
}

task<> ProtoStore::Publish(
    shared_task<TransactionResult> transactionResult,
    uint64_t previousWriteSequenceNumber,
    uint64_t thisWriteSequenceNumber)
{
    co_await transactionResult.when_ready();

    co_await m_writeSequenceNumberBarrier.wait_until_published(
        previousWriteSequenceNumber,
        m_inlineScheduler);

    co_await m_schedulers.LockScheduler->schedule();

    m_writeSequenceNumberBarrier.publish(
        thisWriteSequenceNumber);
}

task<shared_ptr<IIndex>> ProtoStore::GetIndexInternal(
    const string& indexName,
    SequenceNumber sequenceNumber
)
{
    IndexesByNameKey indexesByNameKey;
    indexesByNameKey.set_indexname(
        indexName);

    ReadRequest readRequest;
    readRequest.Key = &indexesByNameKey;
    readRequest.ReadValueDisposition = ReadValueDisposition::DontReadValue;
    readRequest.SequenceNumber = sequenceNumber;

    auto readResult = co_await m_indexesByNameIndex.Index->Read(
        nullptr,
        readRequest);

    // Our internal ability to read an index should not be in doubt.
    // This failure means there's something really wrong with the database.
    if (!readResult)
    {
        std::move(readResult).error().throw_exception();
    }

    // Ordinary index-not-found errors are handled by returning a null index.
    if (readResult->ReadStatus == ReadStatus::NoValue)
    {
        co_return nullptr;
    }

    auto indexesByNameValue = readResult->Value.cast_if<IndexesByNameValue>();

    co_return (co_await GetIndexEntryInternal(
        indexesByNameValue->indexnumber(),
        DoReplayPartitions)
        )->Index;
}

task<IndexNumber> ProtoStore::AllocateIndexNumber()
{
    co_return m_nextIndexNumber.fetch_add(1);
}

operation_task<ProtoIndex> ProtoStore::CreateIndex(
    const CreateIndexRequest& createIndexRequest
)
{
    auto indexNumber = co_await AllocateIndexNumber();

    IndexesByNumberKey indexesByNumberKey;
    IndexesByNumberValue indexesByNumberValue;

    MakeIndexesByNumberRow(
        indexesByNumberKey,
        indexesByNumberValue,
        createIndexRequest.IndexName,
        indexNumber,
        SequenceNumber::Earliest,
        createIndexRequest.KeySchema.KeyDescriptor,
        createIndexRequest.ValueSchema.ValueDescriptor
    );

    auto transactionResult = co_await co_await InternalExecuteTransaction(
        BeginTransactionRequest(),
        [&](auto operation)->status_task<>
    {
        WriteOperationMetadata metadata;

        co_await co_await operation->AddRow(
            metadata,
            m_indexesByNumberIndex.Index,
            &indexesByNumberKey,
            &indexesByNumberValue
        );

        IndexesByNameKey indexesByNameKey;
        IndexesByNameValue indexesByNameValue;

        indexesByNameKey.set_indexname(
            createIndexRequest.IndexName);
        indexesByNameValue.set_indexnumber(
            indexNumber);

        co_await co_await operation->AddRow(
            metadata,
            m_indexesByNameIndex.Index,
            &indexesByNameKey,
            &indexesByNameValue
        );

        co_return{};
    });

    auto indexEntry = co_await GetIndexEntryInternal(
        indexNumber,
        DoReplayPartitions);

    co_return ProtoIndex(
        indexEntry->Index);
}

task<const ProtoStore::IndexEntry*> ProtoStore::GetIndexEntryInternal(
    google::protobuf::uint64 indexNumber,
    bool doReplayPartitions
)
{
    // Look for the index using a read lock
    {
        auto lock = co_await m_indexesByNumberLock.reader().scoped_lock_async();
        auto indexIterator = m_indexesByNumber.find(
            indexNumber);
        if (indexIterator != m_indexesByNumber.end())
        {
            co_return &indexIterator->second;
        }
    }

    // The index didn't exist, so it's likely we'll have to create it.
    // Gather the information about the index before we create it.
    IndexesByNumberKey indexesByNumberKey;
    indexesByNumberKey.set_indexnumber(
        indexNumber);

    ReadRequest readRequest;
    readRequest.Key = &indexesByNumberKey;
    readRequest.SequenceNumber = SequenceNumber::Latest;
    readRequest.ReadValueDisposition = ReadValueDisposition::ReadValue;
    auto readResult = co_await m_indexesByNumberIndex.Index->Read(
        nullptr,
        readRequest);

    if (!readResult)
    {
        readResult.error().throw_exception();
    }

    auto indexesByNumberValue = readResult->Value.cast_if<IndexesByNumberValue>();
    
    // Look for the index using a write lock, and create the index if it doesn't exist.
    {
        auto lock = co_await m_indexesByNumberLock.writer().scoped_lock_async();
        auto indexIterator = m_indexesByNumber.find(
            indexNumber);
        if (indexIterator != m_indexesByNumber.end())
        {
            co_return &indexIterator->second;
        }

        auto indexEntry = MakeIndex(
            indexesByNumberKey,
            *indexesByNumberValue);

        if (doReplayPartitions)
        {
            co_await ReplayPartitionsForIndex(
                indexEntry);
        }
        else
        {
            co_await indexEntry.DataSources->UpdatePartitions(
                {},
                {});
        }

        m_indexesByNumber[indexNumber] = indexEntry;

        co_return &m_indexesByNumber[indexNumber];
    }
}

void ProtoStore::MakeIndexesByNumberRow(
    IndexesByNumberKey& indexesByNumberKey,
    IndexesByNumberValue& indexesByNumberValue,
    const IndexName& indexName,
    IndexNumber indexNumber,
    SequenceNumber createSequenceNumber,
    const Descriptor* keyDescriptor,
    const Descriptor* valueDescriptor
)
{
    indexesByNumberKey.Clear();
    indexesByNumberValue.Clear();

    indexesByNumberKey.set_indexnumber(indexNumber);

    indexesByNumberValue.set_indexname(indexName);
    indexesByNumberValue.set_createsequencenumber(ToUint64(createSequenceNumber));

    Schema::MakeMessageDescription(
        *(indexesByNumberValue.mutable_schema()->mutable_key()->mutable_description()),
        keyDescriptor);

    Schema::MakeMessageDescription(
        *(indexesByNumberValue.mutable_schema()->mutable_value()->mutable_description()),
        valueDescriptor);
}

ProtoStore::IndexEntry ProtoStore::MakeIndex(
    const IndexesByNumberKey& indexesByNumberKey,
    const IndexesByNumberValue& indexesByNumberValue
)
{
    auto keyMessageFactory = Schema::MakeMessageFactory(
        indexesByNumberValue.schema().key().description());
    
    auto valueMessageFactory = Schema::MakeMessageFactory(
        indexesByNumberValue.schema().value().description());

    auto index = make_shared<Index>(
        indexesByNumberValue.indexname(),
        indexesByNumberKey.indexnumber(),
        ToSequenceNumber(indexesByNumberValue.createsequencenumber()),
        keyMessageFactory,
        valueMessageFactory,
        m_unresolvedTransactionsTracker.get()
        );

    auto makeMemoryTable = [=]()
    {
        return make_shared<MemoryTable>(
            &*index->GetKeyComparer());
    };

    auto indexDataSources = make_shared<IndexDataSources>(
        index,
        makeMemoryTable);

    auto mergeGenerator = make_shared<IndexPartitionMergeGenerator>();

    return IndexEntry
    {
        .IndexNumber = indexesByNumberKey.indexnumber(),
        .DataSources = indexDataSources,
        .Index = index,
        .MergeGenerator = mergeGenerator,
    };
}

task<shared_ptr<IPartition>> ProtoStore::OpenPartitionForIndex(
    const shared_ptr<IIndex>& index,
    ExtentName headerExtentName)
{
    if (m_activePartitions.contains(headerExtentName))
    {
        co_return m_activePartitions[headerExtentName];
    }

    auto dataExtentName = MakePartitionDataExtentName(
        headerExtentName);

    auto partition = make_shared<Partition>(
        index->GetKeyComparer(),
        index->GetKeyFactory(),
        index->GetValueFactory(),
        m_messageAccessor,
        ExtentLocation
        {
            .extentName = headerExtentName,
            .extentOffset = 0,
        },
        dataExtentName
        );

    co_await partition->Open();

    if (m_integrityChecks.contains(IntegrityCheck::CheckPartitionOnOpen))
    {
        auto errorList = co_await partition->CheckIntegrity(
            IntegrityCheckError{});
        if (!errorList.empty())
        {
            throw IntegrityException(errorList);
        }
    }
    m_activePartitions[headerExtentName] = partition;

    co_return partition;
}

task<> ProtoStore::Checkpoint()
{
    co_await m_encompassingCheckpointTask.spawn(
        InternalCheckpoint());
}

task<> ProtoStore::InternalCheckpoint()
{
    // Switch to a new log at the start of the checkpoint to make it more likely
    // to be able to clean up the current log at the next checkpoint.
    co_await SwitchToNewLog();

    vector<task<>> checkpointTasks;

    {
        auto lock = co_await m_indexesByNumberLock.reader().scoped_lock_async();

        for (auto index : m_indexesByNumber)
        {
            checkpointTasks.push_back(
                Checkpoint(
                    index.second));
        }
    }

    for (auto& task : checkpointTasks)
    {
        co_await task.when_ready();
    }

    for (auto& task : checkpointTasks)
    {
        co_await task;
    }
}

task<> ProtoStore::Merge()
{
    co_await m_mergeTask.spawn();
}

task<> ProtoStore::InternalMerge()
{
    IndexPartitionMergeGenerator mergeGenerator;
    IndexMerger merger(
        this,
        &mergeGenerator);

    co_await merger.Merge(
        m_defaultMergeParameters);
    co_await merger.Join();
}

task<> ProtoStore::SwitchToNewLog()
{
    task<> postUpdateHeaderTask;

    co_await UpdateHeader([this, &postUpdateHeaderTask](auto header) -> task<>
    {
        postUpdateHeaderTask = co_await m_logManager->Checkpoint(
            header);
    });

    co_await postUpdateHeaderTask;
}

task<> ProtoStore::Checkpoint(
    IndexEntry indexEntry
)
{
    auto loggedCheckpoint = co_await indexEntry.DataSources->StartCheckpoint();

    if (!loggedCheckpoint.checkpointnumber_size())
    {
        co_return;
    }

    ExtentName headerExtentName;
    ExtentName dataExtentName;
    shared_ptr<IPartitionWriter> partitionWriter;

    co_await OpenPartitionWriter(
        indexEntry.IndexNumber,
        indexEntry.Index->GetIndexName(),
        0,
        headerExtentName,
        dataExtentName,
        partitionWriter);

    auto writeRowsResult = co_await indexEntry.DataSources->Checkpoint(
        loggedCheckpoint,
        partitionWriter);

    auto writeSequenceNumber = ToSequenceNumber(
        m_nextWriteSequenceNumber.fetch_add(1));

    co_await InternalExecuteTransaction(
        BeginTransactionRequest{},
        [&](auto operation) -> status_task<>
    {
        co_await LogCommitExtent(
            operation->LogRecord(),
            headerExtentName
        );

        co_await LogCommitExtent(
            operation->LogRecord(),
            dataExtentName
        );

        auto addedLoggedCheckpoint = operation->LogRecord().mutable_extras()->add_loggedactions()->mutable_loggedcheckpoints();
        auto addedLoggedUpdatePartitions = operation->LogRecord().mutable_extras()->add_loggedactions()->mutable_loggedupdatepartitions();

        auto highestCheckpointNumber = loggedCheckpoint.checkpointnumber(0);
        for (auto checkpointIndex = 1; checkpointIndex < loggedCheckpoint.checkpointnumber_size(); checkpointIndex++)
        {
            highestCheckpointNumber =
                std::max(
                    highestCheckpointNumber,
                    loggedCheckpoint.checkpointnumber(checkpointIndex)
                );
        }

        LoggedPartitionsData* addedLoggedPartitionsData = nullptr;
        if (indexEntry.IndexNumber == m_partitionsIndex.IndexNumber)
        {
            addedLoggedPartitionsData = operation->LogRecord().mutable_extras()->add_loggedactions()->mutable_loggedpartitionsdata();

            // Set the checkpoint number of the added logged partitions data to be the greatest
            // checkpoint number being written.
            addedLoggedPartitionsData->set_partitionstablecheckpointnumber(
                highestCheckpointNumber);
        }

        PartitionsKey partitionsKey;
        partitionsKey.set_indexnumber(indexEntry.IndexNumber);
        *partitionsKey.mutable_headerextentname() = headerExtentName;
        PartitionsValue partitionsValue;
        *partitionsValue.mutable_dataextentname() = dataExtentName;
        partitionsValue.set_size(writeRowsResult.writtenDataSize);
        partitionsValue.set_level(0);
        partitionsValue.set_checkpointnumber(highestCheckpointNumber);

        {
            auto updatePartitionsMutex = co_await m_updatePartitionsMutex.scoped_lock_async();

            auto partitionKeyValues = co_await GetPartitionsForIndex(
                indexEntry.IndexNumber);
            partitionKeyValues.push_back(std::make_tuple(
                partitionsKey,
                partitionsValue));

            vector<ExtentName> headerExtentNames;

            for (auto& partitionKeyValue : partitionKeyValues)
            {
                auto existingHeaderExtentName = get<PartitionsKey>(partitionKeyValue).headerextentname();

                if (indexEntry.IndexNumber == m_partitionsIndex.IndexNumber)
                {
                    *addedLoggedPartitionsData->add_headerextentnames() =
                        existingHeaderExtentName;
                }

                headerExtentNames.push_back(
                    move(existingHeaderExtentName));
            }

            addedLoggedCheckpoint->CopyFrom(
                loggedCheckpoint);
            addedLoggedUpdatePartitions->set_indexnumber(
                indexEntry.IndexNumber);

            co_await operation->AddRow(
                WriteOperationMetadata(),
                m_partitionsIndex.Index,
                &partitionsKey,
                &partitionsValue);

            co_await operation->Commit();

            auto partitions = co_await OpenPartitionsForIndex(
                indexEntry.Index,
                headerExtentNames);

            co_await indexEntry.DataSources->UpdatePartitions(
                loggedCheckpoint,
                partitions);

            co_return{};
        }
    });
}

task<vector<std::tuple<Serialization::PartitionsKey, Serialization::PartitionsValue>>> ProtoStore::GetPartitionsForIndex(
    IndexNumber indexNumber)
{
    PartitionsKey partitionsKeyLow;
    partitionsKeyLow.set_indexnumber(indexNumber);

    PartitionsKey partitionsKeyHigh;
    partitionsKeyHigh.set_indexnumber(indexNumber + 1);

    EnumerateRequest enumerateRequest;
    enumerateRequest.KeyLow = &partitionsKeyLow;
    enumerateRequest.KeyLowInclusivity = Inclusivity::Inclusive;
    enumerateRequest.KeyHigh = &partitionsKeyHigh;
    enumerateRequest.KeyHighInclusivity = Inclusivity::Exclusive;
    enumerateRequest.SequenceNumber = SequenceNumber::LatestCommitted;
    enumerateRequest.Index = ProtoIndex{ m_partitionsIndex.Index };

    auto enumeration = m_partitionsIndex.Index->Enumerate(
        nullptr,
        enumerateRequest);

    vector<std::tuple<PartitionsKey, PartitionsValue>> result;

    for(auto iterator = co_await enumeration.begin();
        iterator != enumeration.end();
        co_await ++iterator)
    {
        result.push_back(
            std::make_tuple(
                *((*iterator)->Key.cast_if<PartitionsKey>()),
                *((*iterator)->Value.cast_if<PartitionsValue>())));
    }

    co_return result;
}

task<vector<shared_ptr<IPartition>>> ProtoStore::OpenPartitionsForIndex(
    const shared_ptr<IIndex>& index,
    const vector<ExtentName>& headerExtentNames)
{
    vector<shared_ptr<IPartition>> partitions;

    for (auto headerExtentName : headerExtentNames)
    {
        auto partition = co_await OpenPartitionForIndex(
            index,
            headerExtentName);

        partitions.push_back(
            partition);
    }

    co_return partitions;
}

task<> ProtoStore::Join()
{
    vector<task<>> joinTasks;

    for (auto index : m_indexesByNumber)
    {
        joinTasks.push_back(
            index.second.Index->Join());
    }

    for (auto& task : joinTasks)
    {
        co_await task.when_ready();
    }

    for (auto& task : joinTasks)
    {
        co_await task;
    }

    co_await AsyncScopeMixin::Join();
}

task<> ProtoStore::AllocatePartitionExtents(
    IndexNumber indexNumber,
    IndexName indexName,
    LevelNumber levelNumber,
    ExtentName& out_partitionHeaderExtentName,
    ExtentName& out_partitionDataExtentName)
{
    auto partitionNumber = m_nextPartitionNumber.fetch_add(1);

    out_partitionHeaderExtentName = MakePartitionHeaderExtentName(
        indexNumber,
        partitionNumber,
        levelNumber,
        indexName);
    out_partitionDataExtentName = MakePartitionDataExtentName(
        out_partitionHeaderExtentName);

    LogRecord logRecord;
    *logRecord.mutable_extras()
        ->add_loggedactions()
        ->mutable_loggedcreateextent()
        ->mutable_extentname() = out_partitionHeaderExtentName;
    *logRecord.mutable_extras()
        ->add_loggedactions()
        ->mutable_loggedcreateextent()
        ->mutable_extentname() = out_partitionDataExtentName;
    logRecord.mutable_extras()
        ->add_loggedactions()
        ->mutable_loggedcreatepartition()
        ->set_partitionnumber(partitionNumber);

    co_await WriteLogRecord(
        logRecord);
}

task<> ProtoStore::OpenPartitionWriter(
    IndexNumber indexNumber,
    IndexName indexName,
    LevelNumber levelNumber,
    ExtentName& out_headerExtentName,
    ExtentName& out_dataExtentName,
    shared_ptr<IPartitionWriter>& out_partitionWriter
)
{
    co_await AllocatePartitionExtents(
        indexNumber,
        indexName,
        levelNumber,
        out_headerExtentName,
        out_dataExtentName);

    auto headerWriter = co_await m_messageStore->OpenExtentForSequentialWriteAccess(
        out_headerExtentName);
    auto dataWriter = co_await m_messageStore->OpenExtentForSequentialWriteAccess(
        out_dataExtentName);

    out_partitionWriter = make_shared<PartitionWriter>(
        PartitionWriterParameters(),
        dataWriter,
        headerWriter
        );
}

cppcoro::async_mutex_scoped_lock_operation ProtoStore::AcquireUpdatePartitionsLock(
)
{
    return m_updatePartitionsMutex.scoped_lock_async();
}

task<> ProtoStore::UpdatePartitionsForIndex(
    IndexNumber indexNumber,
    cppcoro::async_mutex_lock& acquiredUpdatePartitionsLock
)
{
    IndexEntry indexEntry;
    {
        auto readLock = co_await m_indexesByNumberLock.reader().scoped_lock_async();
        if (!m_indexesByNumber.contains(indexNumber))
        {
            co_return;
        }
        indexEntry = m_indexesByNumber[indexNumber];
    }

    co_await ReplayPartitionsForIndex(
        indexEntry);
}

shared_ptr<IIndex> ProtoStore::GetMergesIndex()
{
    return m_mergesIndex.Index;
}

shared_ptr<IIndex> ProtoStore::GetMergeProgressIndex()
{
    return m_mergeProgressIndex.Index;
}

shared_ptr<IIndex> ProtoStore::GetPartitionsIndex()
{
    return m_partitionsIndex.Index;
}

shared_ptr<IIndex> ProtoStore::GetUnresolvedTransactionsIndex()
{
    return m_unresolvedTransactionIndex.Index;
}

task<> ProtoStore::LogCommitExtent(
    LogRecord& logRecord,
    ExtentName extentName
)
{
    *logRecord
        .mutable_extras()
        ->add_loggedactions()
        ->mutable_loggedcommitextent()
        ->mutable_extentname() = move(extentName);

    co_return;
}

task<> ProtoStore::LogDeleteExtentPendingPartitionsUpdated(
    LogRecord& logRecord,
    ExtentName extentName,
    CheckpointNumber partitionsTableCheckpointNumber
)
{
    auto loggedDeleteExtentPendingPartitionsUpdated = logRecord
        .mutable_extras()
        ->add_loggedactions()
        ->mutable_loggeddeleteextentpendingpartitionsupdated();

    *loggedDeleteExtentPendingPartitionsUpdated
        ->mutable_extentname() = move(extentName);
    loggedDeleteExtentPendingPartitionsUpdated->set_partitionstablecheckpointnumber(
        partitionsTableCheckpointNumber);

    co_return;
}

Schedulers Schedulers::Default()
{
    static shared_ptr<IScheduler> scheduler = std::make_shared<DefaultScheduler<cppcoro::static_thread_pool>>();

    static Schedulers schedulers =
    {
        .LockScheduler = scheduler,
        .IoScheduler = scheduler,
        .ComputeScheduler = scheduler,
    };

    return schedulers;
}

Schedulers Schedulers::Inline()
{
    static shared_ptr<IScheduler> scheduler = std::make_shared<DefaultScheduler<cppcoro::inline_scheduler>>();

    static Schedulers schedulers =
    {
        .LockScheduler = scheduler,
        .IoScheduler = scheduler,
        .ComputeScheduler = scheduler,
    };

    return schedulers;
}

}