#include "ExtentName.h"
#include "HeaderAccessor.h"
#include "IndexDataSourcesImpl.h"
#include "IndexImpl.h"
#include "IndexMerger.h"
#include "IndexPartitionMergeGenerator.h"
#include "MemoryTableImpl.h"
#include "MessageStore.h"
#include "PartitionImpl.h"
#include "PartitionWriterImpl.h"
#include "Phantom.System/async_value_source.h"
#include "ProtoStore.h"
#include "ProtoStoreInternal.pb.h"
#include "RandomMessageAccessor.h"
#include "src/ProtoStoreInternal_generated.h"
#include "Schema.h"
#include "StandardTypes.h"
#include "UnresolvedTransactionsTracker.h"
#include <algorithm>
#include <cppcoro/sync_wait.hpp>

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

    if (!m_header)
    {
        throw std::range_error("Invalid database");
    }

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

    m_replayedWrites = {};

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
        LoggedCheckpointT(),
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

task<> ProtoStore::Replay(
    const ExtentName& logExtent,
    const FlatBuffers::LogExtentNameT* extentName
)
{
    auto logReader = co_await m_messageStore->OpenExtentForSequentialReadAccess(
        logExtent
    );

    while (true)
    {
        FlatMessage<LogRecord> logRecordMessage{ co_await logReader->Read() };

        if (!logRecordMessage)
        {
            co_return;
        }

        co_await m_logManager->Replay(
            extentName,
            logRecordMessage.get()
        );

        co_await Replay(
            logRecordMessage);
    }
}

task<> ProtoStore::Replay(
    const FlatMessage<LogRecord>& logRecord)
{
    if (!logRecord->log_entry())
    {
        co_return;
    }

    for(auto logEntryIndex = 0; logEntryIndex < logRecord->log_entry()->size(); logEntryIndex++)
    {
        auto getMessage = [&]<typename T>(tag<T>)
        {
            return FlatMessage{ logRecord, logRecord->log_entry()->GetAs<T>(logEntryIndex) };
        };

        switch (logRecord->log_entry_type()->Get(logEntryIndex))
        {
        case LogEntry::LoggedRowWrite:
            co_await Replay(getMessage(tag<LoggedRowWrite>()));
            break;

        case LogEntry::LoggedCreateIndex:
            co_await Replay(getMessage(tag<LoggedCreateIndex>()));
            break;

        case LogEntry::LoggedPartitionsData:
            co_await Replay(getMessage(tag<LoggedPartitionsData>()));
            break;

        case LogEntry::LoggedCreatePartition:
            co_await Replay(getMessage(tag<LoggedCreatePartition>()));
            break;

        case LogEntry::LoggedCommitLocalTransaction:
            co_await Replay(getMessage(tag<LoggedCommitLocalTransaction>()));
            break;

        case LogEntry::LoggedUpdatePartitions:
            co_await Replay(getMessage(tag<LoggedUpdatePartitions>()));
            break;
        }
    }
}

task<> ProtoStore::Replay(
    const FlatMessage<LoggedRowWrite>& logRecord
)
{
    m_localTransactionNumber = std::max(
        m_localTransactionNumber.load(std::memory_order_relaxed),
        logRecord->local_transaction_id() + 1);

    m_replayedWrites[logRecord->local_transaction_id()][logRecord->write_id()] = logRecord;
    co_return;
}

task<> ProtoStore::Replay(
    const FlatMessage<LoggedCommitLocalTransaction>& logRecord
)
{
    m_localTransactionNumber = std::max(
        m_localTransactionNumber.load(std::memory_order_relaxed),
        logRecord->local_transaction_id() + 1);

    auto& writes = m_replayedWrites[logRecord->local_transaction_id()];
    for (auto write_id : *logRecord->write_id())
    {
        auto& write = writes.at(write_id);
        
        auto index = co_await GetIndexEntryInternal(
            write->index_number(),
            DoReplayPartitions);

        co_await index->DataSources->Replay(
            write);
    }
    m_replayedWrites.erase(logRecord->local_transaction_id());
}

task<> ProtoStore::Replay(
    const FlatMessage<LoggedCreateIndex>& logRecord)
{
    m_nextIndexNumber = std::max(
        m_nextIndexNumber.load(),
        logRecord->index_number() + 1);
    co_return;
}

task<> ProtoStore::Replay(
    const FlatMessage<LoggedPartitionsData>& logRecord)
{
    std::vector<ExtentName> headerExtentNames;

    if (logRecord->header_extent_names())
    {
        std::ranges::transform(
            *logRecord->header_extent_names(),
            std::back_inserter(headerExtentNames),
            [&](const auto& extentName)
        {
            return MakeExtentName(*extentName);
        });
    }

    auto partitions = co_await OpenPartitionsForIndex(
        m_partitionsIndex.Index,
        headerExtentNames);

    co_await m_partitionsIndex.DataSources->UpdatePartitions(
        LoggedCheckpointT(),
        partitions);

    // If DataSources changed its partitions list, 
    // then all other indexes may have changed.
    co_await ReplayPartitionsForOpenedIndexes();
}

task<> ProtoStore::Replay(
    const FlatMessage<LoggedCreatePartition>& logRecord)
{
    m_nextPartitionNumber = std::max(
        m_nextPartitionNumber.load(),
        logRecord->partition_number() + 1);

    co_return;
}

task<> ProtoStore::Replay(
    const FlatMessage<LoggedUpdatePartitions>& logRecord)
{
    if (m_indexesByNumber.contains(logRecord->index_number()))
    {
        co_await ReplayPartitionsForIndex(
            m_indexesByNumber[logRecord->index_number()]
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
    flatbuffers::FlatBufferBuilder m_logRecordBuilder;
    
    std::vector<FlatBuffers::LogEntry> m_logEntryTypes;
    std::vector<flatbuffers::Offset<void>> m_logEntries;
    uint32_t m_nextWriteId = 1;
    std::vector<uint32_t> m_writeIdsToCommit;

    LocalTransactionNumber m_localTransactionNumber;
    shared_ptr<DelayedMemoryTableTransactionOutcome> m_delayedOperationOutcome;
    SequenceNumber m_readSequenceNumber;
    SequenceNumber m_initialWriteSequenceNumber;
    shared_task<CommitResult> m_commitTask;

public:
    LocalTransaction(
        LocalTransactionNumber localTransactionNumber,
        MemoryTableTransactionSequenceNumber memoryTableTransactionSequenceNumber,
        ProtoStore& protoStore,
        SequenceNumber readSequenceNumber,
        SequenceNumber initialWriteSequenceNumber
    )
    :
        m_localTransactionNumber(localTransactionNumber),
        m_protoStore(protoStore),
        m_readSequenceNumber(readSequenceNumber),
        m_initialWriteSequenceNumber(initialWriteSequenceNumber),
        m_delayedOperationOutcome(std::make_shared<DelayedMemoryTableTransactionOutcome>(
            memoryTableTransactionSequenceNumber
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

    virtual void BuildLogRecord(
        LogEntry logEntry,
        std::function<Offset<void>(flatbuffers::FlatBufferBuilder&)> builder
    ) override
    {
        auto result = builder(m_logRecordBuilder);
        m_logEntryTypes.push_back(logEntry);
        m_logEntries.push_back(result);
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

    virtual operation_task<FlatMessage<LoggedRowWrite>> AddRowInternal(
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
        auto writeId = m_nextWriteId++;

        FlatMessage<LoggedRowWrite> loggedRowWrite;

        auto createLoggedRowWrite = [&](CheckpointNumber checkpointNumber) -> task<FlatMessage<LoggedRowWrite>>
        {
            auto unownedKey = key.pack_unowned();
            auto unownedValue = value.pack_unowned();

            auto keySpan = get_int8_t_span(unownedKey.as_bytes_if());
            auto valueSpan = get_int8_t_span(unownedValue.as_bytes_if());

            flatbuffers::FlatBufferBuilder loggedRowWriteBuilder;

            Offset<flatbuffers::Vector<int8_t>> keyOffset = loggedRowWriteBuilder.CreateVector<int8_t>(
                keySpan);

            Offset<flatbuffers::Vector<int8_t>> valueOffset;
            Offset<flatbuffers::Vector<int8_t>> transactionIdOffset;

            if (valueSpan.data())
            {
                valueOffset = loggedRowWriteBuilder.CreateVector<int8_t>(
                    valueSpan);
            }
            if (writeOperationMetadata.TransactionId)
            {
                transactionIdOffset = loggedRowWriteBuilder.CreateVector<int8_t>(
                    get_int8_t_span(get_byte_span(*writeOperationMetadata.TransactionId)));
            }

            auto loggedRowWriteOffset = FlatBuffers::CreateLoggedRowWrite(
                loggedRowWriteBuilder,
                protoIndex.m_index->GetIndexNumber(),
                ToUint64(writeSequenceNumber),
                checkpointNumber,
                keyOffset,
                valueOffset,
                transactionIdOffset,
                m_localTransactionNumber,
                writeId
            ).Union();

            auto logEntryType = LogEntry::LoggedRowWrite;
            auto logEntryTypeOffset = loggedRowWriteBuilder.CreateVector(
                &logEntryType,
                1);

            auto logEntryOffset = loggedRowWriteBuilder.CreateVector(
                &loggedRowWriteOffset,
                1);

            auto logRecordOffset = FlatBuffers::CreateLogRecord(
                loggedRowWriteBuilder,
                logEntryTypeOffset,
                logEntryOffset);

            loggedRowWriteBuilder.Finish(
                logRecordOffset);

            FlatMessage<LogRecord> logRecord(loggedRowWriteBuilder);

            logRecord = co_await m_protoStore.m_logManager->WriteLogRecord(
                std::move(logRecord));

            co_return loggedRowWrite = FlatMessage<LoggedRowWrite>
            {
                std::move(logRecord),
                logRecord->log_entry()->GetAs<LoggedRowWrite>(0),
            };
        };

        auto checkpointNumber = co_await index->AddRow(
            readSequenceNumber,
            createLoggedRowWrite,
            m_delayedOperationOutcome
        );

        if (!checkpointNumber)
        {
            co_return std::unexpected{ checkpointNumber.error() };
        }

        m_writeIdsToCommit.push_back(writeId);

        //if (writeOperationMetadata.TransactionId)
        //{
        //    co_await m_protoStore.m_unresolvedTransactionsTracker->LogUnresolvedTransaction(
        //        this,
        //        *loggedRowWrite
        //    );
        //}

        co_return loggedRowWrite;
    }

    virtual operation_task<> AddRow(
        const WriteOperationMetadata& writeOperationMetadata,
        ProtoIndex protoIndex,
        const ProtoValue& key,
        const ProtoValue& value
    ) override
    {
        co_await co_await AddRowInternal(
            writeOperationMetadata,
            protoIndex,
            key,
            value
        );
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
            BuildLogRecord(
                LogEntry::LoggedCommitLocalTransaction,
                [&](auto& builder)
            {
                return FlatBuffers::CreateLoggedCommitLocalTransactionDirect(
                    builder,
                    &m_writeIdsToCommit,
                    m_localTransactionNumber,
                    ToUint64(result.WriteSequenceNumber)
                ).Union();
            });

            auto logRecordOffset = FlatBuffers::CreateLogRecordDirect(
                m_logRecordBuilder,
                &m_logEntryTypes,
                &m_logEntries
            );

            m_logRecordBuilder.Finish(
                logRecordOffset);

            co_await m_protoStore.m_logManager->WriteLogRecord(
                FlatMessage<LogRecord>{ m_logRecordBuilder });
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
        m_localTransactionNumber.fetch_add(1, std::memory_order_relaxed),
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

    IndexesByNameValue indexesByNameValue;
    readResult->Value.unpack(&indexesByNameValue);

    co_return (co_await GetIndexEntryInternal(
        indexesByNameValue.indexnumber(),
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

    IndexesByNumberValue indexesByNumberValue;
    readResult->Value.unpack(&indexesByNumberValue);
    
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
            indexesByNumberValue);

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

    auto headerReader = co_await m_messageStore->OpenExtentForRandomReadAccess(
        headerExtentName);

    auto dataReader = co_await m_messageStore->OpenExtentForRandomReadAccess(
        dataExtentName);
    
    auto partition = make_shared<Partition>(
        index->GetKeyComparer(),
        std::move(headerReader),
        std::move(dataReader)
        );

    co_await partition->Open();

    if (m_integrityChecks.contains(IntegrityCheck::CheckPartitionOnOpen))
    {
        IntegrityCheckError errorPrototype;
        errorPrototype.Location = ExtentLocation
        {
            .extentName = dataExtentName,
        };

        auto errorList = co_await partition->CheckIntegrity(
            errorPrototype);
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

    if (!loggedCheckpoint.checkpoint_number.size())
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
        [&](IInternalTransaction* operation) -> status_task<>
    {
        operation->BuildLogRecord(
            LogEntry::LoggedCommitExtent,
            [&](auto& builder)
        {
            auto headerExtentNameOffset = CreateExtentName(
                builder,
                headerExtentName);

            return FlatBuffers::CreateLoggedCommitExtent(
                builder,
                headerExtentNameOffset
            ).Union();
        });

        operation->BuildLogRecord(
            LogEntry::LoggedCommitExtent,
            [&](auto& builder)
        {
            auto dataExtentNameOffset = CreateExtentName(
                builder,
                dataExtentName);

            return FlatBuffers::CreateLoggedCommitExtent(
                builder,
                dataExtentNameOffset
            ).Union();
        });

        operation->BuildLogRecord(
            loggedCheckpoint);

        FlatBuffers::LoggedUpdatePartitionsT loggedUpdatePartitions;
        loggedUpdatePartitions.index_number = indexEntry.IndexNumber;

        operation->BuildLogRecord(
            loggedUpdatePartitions);

        auto highestCheckpointNumber = loggedCheckpoint.checkpoint_number[0];
        for (auto checkpointIndex = 1; checkpointIndex < loggedCheckpoint.checkpoint_number.size(); checkpointIndex++)
        {
            highestCheckpointNumber =
                std::max(
                    highestCheckpointNumber,
                    loggedCheckpoint.checkpoint_number[checkpointIndex]
                );
        }

        std::optional<LoggedPartitionsDataT> addedLoggedPartitionsData;
        if (indexEntry.IndexNumber == m_partitionsIndex.IndexNumber)
        {
            addedLoggedPartitionsData = LoggedPartitionsDataT();

            // Set the checkpoint number of the added logged partitions data to be the greatest
            // checkpoint number being written.
            addedLoggedPartitionsData->partitions_table_checkpoint_number =
                highestCheckpointNumber;
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
                    addedLoggedPartitionsData->header_extent_names.push_back(
                        std::make_unique<FlatBuffers::ExtentNameT>(MakeExtentName(existingHeaderExtentName)));
                }

                headerExtentNames.push_back(
                    move(existingHeaderExtentName));
            }

            if (addedLoggedPartitionsData)
            {
                operation->BuildLogRecord(
                    *addedLoggedPartitionsData);
            }

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
        PartitionsKey partitionsKey;
        PartitionsValue partitionsValue;

        (*iterator)->Key.unpack(&partitionsKey);
        (*iterator)->Value.unpack(&partitionsValue);
        
        result.push_back(
            std::make_tuple(
                std::move(partitionsKey),
                std::move(partitionsValue)));
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

    FlatBuffers::LogRecordT logRecord;

    FlatBuffers::LoggedCreateExtentT createHeaderExtent;
    createHeaderExtent.extent_name = std::make_unique<FlatBuffers::ExtentNameT>();
    *createHeaderExtent.extent_name = MakeExtentName(out_partitionHeaderExtentName);
    FlatBuffers::LogEntryUnion createHeaderExtentUnion;
    createHeaderExtentUnion.Set(std::move(createHeaderExtent));
    logRecord.log_entry.push_back(std::move(createHeaderExtentUnion));

    FlatBuffers::LoggedCreateExtentT createDataExtent;
    createDataExtent.extent_name = std::make_unique<FlatBuffers::ExtentNameT>();
    *createDataExtent.extent_name = MakeExtentName(out_partitionDataExtentName);
    FlatBuffers::LogEntryUnion createDataExtentUnion;
    createDataExtentUnion.Set(std::move(createDataExtent));
    logRecord.log_entry.push_back(std::move(createDataExtentUnion));

    FlatBuffers::LoggedCreatePartitionT createPartition;
    createPartition.partition_number = partitionNumber;
    FlatBuffers::LogEntryUnion createPartitionUnion;
    createPartitionUnion.Set(std::move(createPartition));
    logRecord.log_entry.push_back(std::move(createPartitionUnion));

    FlatMessage<FlatBuffers::LogRecord> logRecordMessage{ &logRecord };

    co_await m_logManager->WriteLogRecord(
        logRecordMessage);
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