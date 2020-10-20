#include "ExtentName.h"
#include "StandardTypes.h"
#include "ProtoStore.h"
#include "MessageStore.h"
#include "RandomMessageAccessor.h"
#include "HeaderAccessor.h"
#include "Schema.h"
#include "src/ProtoStoreInternal.pb.h"
#include "IndexImpl.h"
#include "IndexDataSourcesImpl.h"
#include "MemoryTableImpl.h"
#include "Phantom.System/async_value_source.h"
#include "PartitionImpl.h"
#include "PartitionWriterImpl.h"
#include <cppcoro/sync_wait.hpp>
#include <cppcoro/when_all.hpp>
#include <cppcoro/on_scope_exit.hpp>
#include "IndexMerger.h"
#include "IndexPartitionMergeGenerator.h"

namespace Phantom::ProtoStore
{

ProtoStore::ProtoStore(
    Schedulers schedulers,
    shared_ptr<IExtentStore> headerExtentStore,
    shared_ptr<IExtentStore> logExtentStore,
    shared_ptr<IExtentStore> dataExtentStore,
    shared_ptr<IExtentStore> dataHeaderExtentStore
)
    :
    m_schedulers(schedulers),
    m_headerExtentStore(move(headerExtentStore)),
    m_logExtentStore(move(logExtentStore)),
    m_dataExtentStore(move(dataExtentStore)),
    m_dataHeaderExtentStore(move(dataHeaderExtentStore)),
    m_headerMessageStore(MakeMessageStore(m_schedulers, m_headerExtentStore)),
    m_logMessageStore(MakeMessageStore(m_schedulers, m_logExtentStore)),
    m_dataMessageStore(MakeMessageStore(m_schedulers, m_dataExtentStore)),
    m_dataHeaderMessageStore(MakeMessageStore(m_schedulers, m_dataHeaderExtentStore)),
    m_headerMessageAccessor(MakeRandomMessageAccessor(m_headerMessageStore)),
    m_dataMessageAccessor(MakeRandomMessageAccessor(m_dataMessageStore)),
    m_dataHeaderMessageAccessor(MakeRandomMessageAccessor(m_dataHeaderMessageStore)),
    m_headerAccessor(MakeHeaderAccessor(m_headerMessageAccessor)),
    m_checkpointTask([=] { return InternalCheckpoint(); }),
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

    Header header;
    header.set_version(1);
    header.set_epoch(1);
    header.set_logalignment(static_cast<google::protobuf::uint32>(
        createRequest.LogAlignment));
    header.set_nextpartitionnumber(1);
    header.set_nextindexnumber(1000);

    co_await m_headerAccessor->WriteHeader(
        header);

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

    co_await m_headerAccessor->ReadHeader(
        m_header);

    m_nextPartitionNumber.store(
        m_header.nextpartitionnumber());
    m_nextIndexNumber.store(
        m_header.nextindexnumber());

    {
        IndexesByNumberKey indexesByNumberKey;
        IndexesByNumberValue indexesByNumberValue;

        MakeIndexesByNumberRow(
            indexesByNumberKey,
            indexesByNumberValue,
            "__System.IndexesByNumber",
            0,
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
            1,
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
            2,
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
            3,
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
            4,
            SequenceNumber::Earliest,
            MergeProgressKey::descriptor(),
            MergeProgressValue::descriptor());

        m_mergeProgressIndex = MakeIndex(
            indexesByNumberKey,
            indexesByNumberValue
        );
    }

    m_indexesByNumber[m_indexesByNumberIndex.IndexNumber] = m_indexesByNumberIndex;
    m_indexesByNumber[m_indexesByNameIndex.IndexNumber] = m_indexesByNameIndex;
    m_indexesByNumber[m_partitionsIndex.IndexNumber] = m_partitionsIndex;
    m_indexesByNumber[m_mergesIndex.IndexNumber] = m_mergesIndex;
    m_indexesByNumber[m_mergeProgressIndex.IndexNumber] = m_mergeProgressIndex;

    m_logManager.emplace(
        m_schedulers,
        m_logExtentStore,
        m_logMessageStore,
        m_header);

    // The first log record for the partitions index will be the actual
    // partitions to use, but the very first time a store is opened
    // it will have no data sources.
    co_await m_partitionsIndex.DataSources->UpdatePartitions(
        {},
        {});

    co_await ReplayPartitionsForOpenedIndexes();

    for (auto logReplayExtentName : m_header.logreplayextentnames())
    {
        co_await Replay(
            logReplayExtentName);
    }

    auto postUpdateHeaderTask = co_await m_logManager->FinishReplay(
        m_header);

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
    std::function<task<>(Header&)> modifier
)
{
    auto lock = co_await m_headerMutex.scoped_lock_async();
        
    auto nextEpoch = m_header.epoch() + 1;
    
    co_await modifier(
        m_header);

    m_header.set_epoch(
        nextEpoch);

    m_header.set_nextindexnumber(
        m_nextIndexNumber.load());

    m_header.set_nextpartitionnumber(
        m_nextPartitionNumber.load());

    co_await m_headerAccessor->WriteHeader(
        m_header);
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

    if (writeMessageResult.DataRange.End == 0)
    {
        m_nextCheckpointLogOffset.store(
            m_checkpointLogSize);
    }
    else if (writeMessageResult.DataRange.End > nextCheckpointLogOffset
        && m_nextCheckpointLogOffset.compare_exchange_weak(
            nextCheckpointLogOffset,
            writeMessageResult.DataRange.End + m_checkpointLogSize))
    {
        spawn(
            Checkpoint());
    }
}

task<> ProtoStore::Replay(
    ExtentName extentName
)
{
    auto logReader = co_await m_logMessageStore->OpenExtentForSequentialReadAccess(
        extentName
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

        co_await indexEntry.DataSources->Replay(
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
    const LoggedCreateExtent& logRecord)
{
    if (logRecord.extentname().has_indexdataextentname()
        && m_nextPartitionNumber.load() <= logRecord.extentname().indexdataextentname().partitionnumber())
    {
        m_nextPartitionNumber.store(
            logRecord.extentname().indexdataextentname().partitionnumber() + 1);
    }

    if (logRecord.extentname().has_indexdataextentname()
        && m_nextPartitionNumber.load() <= logRecord.extentname().indexheaderextentname().partitionnumber())
    {
        m_nextPartitionNumber.store(
            logRecord.extentname().indexheaderextentname().partitionnumber() + 1);
    }

    co_return;
}

task<> ProtoStore::Replay(
    const LoggedCommitExtent& logRecord)
{
    if (logRecord.extentname().has_indexdataextentname()
        && m_nextPartitionNumber.load() <= logRecord.extentname().indexdataextentname().partitionnumber())
    {
        m_nextPartitionNumber.store(
            logRecord.extentname().indexdataextentname().partitionnumber() + 1);
    }

    if (logRecord.extentname().has_indexdataextentname()
        && m_nextPartitionNumber.load() <= logRecord.extentname().indexheaderextentname().partitionnumber())
    {
        m_nextPartitionNumber.store(
            logRecord.extentname().indexheaderextentname().partitionnumber() + 1);
    }

    co_return;
}

task<> ProtoStore::Replay(
    const LoggedDeleteExtent& logRecord)
{
    if (logRecord.extentname().has_indexdataextentname()
        && m_nextPartitionNumber.load() <= logRecord.extentname().indexdataextentname().partitionnumber())
    {
        m_nextPartitionNumber.store(
            logRecord.extentname().indexdataextentname().partitionnumber() + 1);
    }

    if (logRecord.extentname().has_indexdataextentname()
        && m_nextPartitionNumber.load() <= logRecord.extentname().indexheaderextentname().partitionnumber())
    {
        m_nextPartitionNumber.store(
            logRecord.extentname().indexheaderextentname().partitionnumber() + 1);
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
    co_return (co_await GetIndexEntryInternal(indexNumber, DoReplayPartitions)).Index;
}

task<ReadResult> ProtoStore::Read(
    const ReadRequest& readRequest
)
{
    return readRequest.Index.m_index->Read(
        readRequest);
}

async_generator<EnumerateResult> ProtoStore::Enumerate(
    const EnumerateRequest& enumerateRequest
)
{
    return enumerateRequest.Index.m_index->Enumerate(
        enumerateRequest);
}

class Operation
    :
    public IInternalOperation
{
    ProtoStore& m_protoStore;
    ::Phantom::ProtoStore::LogRecord m_logRecord;
    async_value_source<MemoryTableOperationOutcome> m_outcomeValueSource;
    MemoryTableOperationOutcomeTask m_operationOutcomeTask;
    SequenceNumber m_readSequenceNumber;
    SequenceNumber m_initialWriteSequenceNumber;
    shared_task<CommitResult> m_commitTask;

    MemoryTableOperationOutcomeTask GetOperationOutcome()
    {
        co_return co_await m_outcomeValueSource.wait();
    }

    MemoryTableOperationOutcomeTask GetOperationOutcomeAsync(
        WriteOperationMetadata writeOperationMetadata
    )
    {
        auto underlyingOutcome = co_await m_operationOutcomeTask;
        co_return MemoryTableOperationOutcome
        {
            .Outcome = underlyingOutcome.Outcome,
            .WriteSequenceNumber = *writeOperationMetadata.WriteSequenceNumber,
        };
    }

    MemoryTableOperationOutcomeTask GetOperationOutcomeTask(
        WriteOperationMetadata writeOperationMetadata
    )
    {
        if (!writeOperationMetadata.WriteSequenceNumber.has_value())
        {
            return m_operationOutcomeTask;
        }

        return GetOperationOutcomeAsync(
            writeOperationMetadata);
    }

public:
    Operation(
        ProtoStore& protoStore,
        SequenceNumber readSequenceNumber,
        SequenceNumber initialWriteSequenceNumber
    )
    :
        m_protoStore(protoStore),
        m_readSequenceNumber(readSequenceNumber),
        m_initialWriteSequenceNumber(initialWriteSequenceNumber)
    {
        m_operationOutcomeTask = GetOperationOutcome();
        m_commitTask = DelayedCommit();
    }

    ~Operation()
    {
        if (!m_outcomeValueSource.is_set())
        {
            MemoryTableOperationOutcome outcome =
            {
                .Outcome = OperationOutcome::Aborted,
            };

            m_outcomeValueSource.emplace(
                outcome
            );
        }
    }

    ::Phantom::ProtoStore::LogRecord& LogRecord()
    {
        return m_logRecord;
    }

    // Inherited via IOperation
    virtual task<> AddLoggedAction(
        const WriteOperationMetadata& writeOperationMetadata, 
        const Message* loggedAction, 
        LoggedOperationDisposition disposition
    ) override
    {
        return task<>();
    }

    virtual task<> AddRow(
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

        auto operationOutcomeTask = GetOperationOutcomeTask(
            writeOperationMetadata
        );

        auto checkpointNumber = co_await index->AddRow(
            readSequenceNumber,
            key,
            value,
            writeSequenceNumber,
            operationOutcomeTask
        );

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
            checkpointNumber);
    }

    virtual task<> ResolveTransaction(
        const WriteOperationMetadata& writeOperationMetadata, 
        TransactionOutcome outcome
    ) override
    {
        return task<>();
    }

    virtual task<ProtoIndex> GetIndex(
        const GetIndexRequest& getIndexRequest
    ) override
    {
        return m_protoStore.GetIndex(
            getIndexRequest);
    }

    virtual task<ReadResult> Read(
        const ReadRequest& readRequest
    ) override
    {
        return m_protoStore.Read(
            readRequest);
    }

    virtual async_generator<EnumerateResult> Enumerate(
        const EnumerateRequest& enumerateRequest
    ) override
    {
        return m_protoStore.Enumerate(
            enumerateRequest);
    }

    // Inherited via IOperationTransaction
    virtual task<CommitResult> Commit(
    ) override
    {
        co_return co_await m_commitTask;
    }

private:
    shared_task<CommitResult> DelayedCommit()
    {
        MemoryTableOperationOutcome outcome =
        {
            .Outcome = OperationOutcome::Committed,
            .WriteSequenceNumber = m_initialWriteSequenceNumber,
        };

        m_outcomeValueSource.emplace(
            outcome);

        for (auto& addedRow : *m_logRecord.mutable_rows())
        {
            if (!addedRow.sequencenumber())
            {
                addedRow.set_sequencenumber(
                    ToUint64(outcome.WriteSequenceNumber));
            }
        }

        co_await m_protoStore.WriteLogRecord(
            m_logRecord);

        co_return CommitResult
        {
        };
    }
};

task<OperationResult> ProtoStore::ExecuteOperation(
    const BeginTransactionRequest beginRequest,
    OperationVisitor visitor
)
{
    return InternalExecuteOperation(
        beginRequest,
        [=](auto operation)
    {
        return visitor(operation);
    });
}

task<OperationResult> ProtoStore::InternalExecuteOperation(
    const BeginTransactionRequest beginRequest,
    InternalOperationVisitor visitor
)
{
    auto previousWriteSequenceNumber = m_nextWriteSequenceNumber.fetch_add(
        1,
        std::memory_order_acq_rel);
    auto thisWriteSequenceNumber = previousWriteSequenceNumber + 1;

    auto executionOperationTask = InternalExecuteOperation(
        visitor,
        thisWriteSequenceNumber,
        thisWriteSequenceNumber);

    //auto publishTask = Publish(
    //    executionOperationTask,
    //    previousWriteSequenceNumber,
    //    thisWriteSequenceNumber);

    //m_asyncScope.spawn(move(
    //    publishTask));

    co_return co_await executionOperationTask;
}

shared_task<OperationResult> ProtoStore::InternalExecuteOperation(
    InternalOperationVisitor visitor,
    uint64_t readSequenceNumber,
    uint64_t thisWriteSequenceNumber
)
{
    Operation operation(
        *this,
        ToSequenceNumber(readSequenceNumber),
        ToSequenceNumber(thisWriteSequenceNumber));

    co_await visitor(&operation);
    co_await operation.Commit();

    co_return OperationResult{};
}

task<> ProtoStore::Publish(
    shared_task<OperationResult> operationResult,
    uint64_t previousWriteSequenceNumber,
    uint64_t thisWriteSequenceNumber)
{
    co_await operationResult.when_ready();

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
        readRequest);

    if (readResult.ReadStatus == ReadStatus::NoValue)
    {
        co_return nullptr;
    }

    auto indexesByNameValue = readResult.Value.cast_if<IndexesByNameValue>();

    co_return (co_await GetIndexEntryInternal(
        indexesByNameValue->indexnumber(),
        DoReplayPartitions)
        ).Index;
}

task<IndexNumber> ProtoStore::AllocateIndexNumber()
{
    co_return m_nextIndexNumber.fetch_add(1);
}

task<ProtoIndex> ProtoStore::CreateIndex(
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

    co_await InternalExecuteOperation(
        BeginTransactionRequest(),
        [&](auto operation)->task<>
    {
        WriteOperationMetadata metadata;

        co_await operation->AddRow(
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

        co_await operation->AddRow(
            metadata,
            m_indexesByNameIndex.Index,
            &indexesByNameKey,
            &indexesByNameValue
        );

    });

    auto indexEntry = co_await GetIndexEntryInternal(
        indexNumber,
        DoReplayPartitions);

    co_return ProtoIndex(
        indexEntry.Index);
}

task<const ProtoStore::IndexEntry&> ProtoStore::GetIndexEntryInternal(
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
            co_return indexIterator->second;
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
        readRequest);

    auto indexesByNumberValue = readResult.Value.cast_if<IndexesByNumberValue>();

    // Look for the index using a write lock, and create the index if it doesn't exist.
    {
        auto lock = co_await m_indexesByNumberLock.writer().scoped_lock_async();
        auto indexIterator = m_indexesByNumber.find(
            indexNumber);
        if (indexIterator != m_indexesByNumber.end())
        {
            co_return indexIterator->second;
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

        co_return indexEntry;
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
        valueMessageFactory);

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

    ExtentName dataExtentName;
    *dataExtentName.mutable_indexdataextentname() = headerExtentName.indexheaderextentname();

    auto partition = make_shared<Partition>(
        index->GetKeyComparer(),
        index->GetKeyFactory(),
        index->GetValueFactory(),
        m_dataHeaderMessageAccessor,
        m_dataMessageAccessor,
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
    co_await m_checkpointTask.spawn();
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

    co_await cppcoro::when_all(
        move(checkpointTasks));
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

    co_await merger.Merge();
    co_await merger.Join();
}

task<> ProtoStore::SwitchToNewLog()
{
    task<> postUpdateHeaderTask;

    co_await UpdateHeader([this, &postUpdateHeaderTask](auto& header) -> task<>
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
        headerExtentName,
        dataExtentName,
        partitionWriter);

    auto writeRowsResult = co_await indexEntry.DataSources->Checkpoint(
        loggedCheckpoint,
        partitionWriter);

    auto writeSequenceNumber = ToSequenceNumber(
        m_nextWriteSequenceNumber.fetch_add(1));

    co_await InternalExecuteOperation(
        BeginTransactionRequest{},
        [&](auto operation) -> task<>
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

        LoggedPartitionsData* addedLoggedPartitionsData = nullptr;
        if (indexEntry.IndexNumber == m_partitionsIndex.IndexNumber)
        {
            addedLoggedPartitionsData = operation->LogRecord().mutable_extras()->add_loggedactions()->mutable_loggedpartitionsdata();
        }

        PartitionsKey partitionsKey;
        partitionsKey.set_indexnumber(indexEntry.IndexNumber);
        *partitionsKey.mutable_headerextentname() = headerExtentName;
        PartitionsValue partitionsValue;
        *partitionsValue.mutable_dataextentname() = dataExtentName;
        partitionsValue.set_size(writeRowsResult.writtenDataSize);
        partitionsValue.set_level(0);

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
        }
    });
}

task<vector<std::tuple<PartitionsKey, PartitionsValue>>> ProtoStore::GetPartitionsForIndex(
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
        enumerateRequest);

    vector<std::tuple<PartitionsKey, PartitionsValue>> result;

    for(auto iterator = co_await enumeration.begin();
        iterator != enumeration.end();
        co_await ++iterator)
    {
        result.push_back(
            std::make_tuple(
                *((*iterator).Key.cast_if<PartitionsKey>()),
                *((*iterator).Value.cast_if<PartitionsValue>())));
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

    co_await cppcoro::when_all(
        move(joinTasks));

    co_await AsyncScopeMixin::Join();
}

task<> ProtoStore::AllocatePartitionExtents(
    IndexNumber indexNumber,
    IndexName indexName,
    ExtentName& out_partitionHeaderExtentName,
    ExtentName& out_partitionDataExtentName)
{
    auto partitionNumber = m_nextPartitionNumber.fetch_add(1);

    IndexExtentName indexExtentName;
    indexExtentName.set_indexnumber(indexNumber);
    indexExtentName.set_indexname(indexName);
    indexExtentName.set_partitionnumber(partitionNumber);

    *out_partitionDataExtentName.mutable_indexheaderextentname() = indexExtentName;
    *out_partitionDataExtentName.mutable_indexdataextentname() = move(indexExtentName);

    LogRecord logRecord;
    *logRecord.mutable_extras()
        ->add_loggedactions()
        ->mutable_loggedcreateextent()
        ->mutable_extentname() = out_partitionHeaderExtentName;
    *logRecord.mutable_extras()
        ->add_loggedactions()
        ->mutable_loggedcreateextent()
        ->mutable_extentname() = out_partitionDataExtentName;

    co_await WriteLogRecord(
        logRecord);
}

task<> ProtoStore::OpenPartitionWriter(
    IndexNumber indexNumber,
    IndexName indexName,
    ExtentName& out_headerExtentName,
    ExtentName& out_dataExtentName,
    shared_ptr<IPartitionWriter>& out_partitionWriter
)
{
    co_await AllocatePartitionExtents(
        indexNumber,
        indexName,
        out_headerExtentName,
        out_dataExtentName);

    auto dataWriter = co_await m_dataMessageStore->OpenExtentForSequentialWriteAccess(
        out_dataExtentName);
    auto headerWriter = co_await m_dataHeaderMessageStore->OpenExtentForSequentialWriteAccess(
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

task<shared_ptr<IIndex>> ProtoStore::GetMergesIndex()
{
    co_return m_mergesIndex.Index;
}

task<shared_ptr<IIndex>> ProtoStore::GetMergeProgressIndex()
{
    co_return m_mergeProgressIndex.Index;
}

task<shared_ptr<IIndex>> ProtoStore::GetPartitionsIndex()
{
    co_return m_partitionsIndex.Index;
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