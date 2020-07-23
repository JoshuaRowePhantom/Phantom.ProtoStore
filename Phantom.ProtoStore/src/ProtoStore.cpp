#include "ProtoStore.h"
#include "StandardTypes.h"
#include "MessageStore.h"
#include "RandomMessageAccessor.h"
#include "HeaderAccessor.h"
#include "Schema.h"
#include "src/ProtoStoreInternal.pb.h"
#include "IndexImpl.h"
#include "Phantom.System/async_value_source.h"
#include "PartitionImpl.h"
#include "PartitionWriterImpl.h"
#include <cppcoro/sync_wait.hpp>
#include <cppcoro/when_all.hpp>
#include <cppcoro/on_scope_exit.hpp>

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
    m_headerAccessor(MakeHeaderAccessor(m_headerMessageAccessor))
{
    m_writeSequenceNumberBarrier.publish(0);
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
    header.set_nextdataextentnumber(1);
    header.set_nextindexnumber(1000);

    co_await m_headerAccessor->WriteHeader(
        header);

    co_await Open(
        createRequest);
}

task<> ProtoStore::Open(
    const OpenProtoStoreRequest& openRequest)
{
    m_schedulers = openRequest.Schedulers;
    m_checkpointLogSize = openRequest.CheckpointLogSize;
    m_nextCheckpointLogOffset.store(m_checkpointLogSize);

    co_await m_headerAccessor->ReadHeader(
        m_header);

    m_nextDataExtentNumber.store(
        m_header.nextdataextentnumber());
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

    m_indexesByNumber[m_indexesByNumberIndex->GetIndexNumber()] = m_indexesByNumberIndex;
    m_indexesByNumber[m_indexesByNameIndex->GetIndexNumber()] = m_indexesByNameIndex;
    m_indexesByNumber[m_partitionsIndex->GetIndexNumber()] = m_partitionsIndex;

    m_logManager.emplace(
        m_schedulers,
        m_logExtentStore,
        m_logMessageStore,
        m_header);

    for (auto logReplayExtentNumber : m_header.logreplayextentnumbers())
    {
        co_await Replay(
            logReplayExtentNumber);
    }

    auto postUpdateHeaderTask = co_await m_logManager->FinishReplay(
        m_header);

    co_await UpdateHeader([](auto) -> task<> { co_return; });

    co_await postUpdateHeaderTask;
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

    m_header.set_nextdataextentnumber(
        m_nextDataExtentNumber.load());

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
        m_asyncScope.spawn(
            Checkpoint());
    }
}

task<> ProtoStore::Replay(
    ExtentNumber extentNumber
)
{
    auto logReader = co_await m_logMessageStore->OpenExtentForSequentialReadAccess(
        extentNumber
    );

    while (true)
    {
        try
        {
            LogRecord logRecord;
            co_await logReader->Read(
                logRecord);

            co_await m_logManager->Replay(
                extentNumber,
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
        auto index = co_await GetIndexInternal(
            loggedRowWrite.indexnumber());

        co_await index->Replay(
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
    const LoggedCreateDataExtent& logRecord)
{
    if (m_nextDataExtentNumber.load() <= logRecord.extentnumber())
    {
        m_nextDataExtentNumber.store(logRecord.extentnumber() + 1);
    }
    co_return;
}

task<> ProtoStore::Replay(
    const LoggedCommitDataExtent& logRecord)
{
    if (m_nextDataExtentNumber.load() <= logRecord.extentnumber())
    {
        m_nextDataExtentNumber.store(logRecord.extentnumber() + 1);
    }
    co_return;
}

task<> ProtoStore::Replay(
    const LoggedDeleteDataExtent& logRecord)
{
    if (m_nextDataExtentNumber.load() <= logRecord.extentnumber())
    {
        m_nextDataExtentNumber.store(logRecord.extentnumber() + 1);
    }
    co_return;
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
        this,
        &*index
    );
}

task<ReadResult> ProtoStore::Read(
    ReadRequest& readRequest
)
{
    return readRequest.Index.m_index->Read(
        readRequest);
}

class Operation
    :
    public IOperationTransaction
{
    friend class ProtoStore;

    ProtoStore& m_protoStore;
    unique_ptr<LogRecord> m_logRecord;
    async_value_source<MemoryTableOperationOutcome> m_outcomeValueSource;
    MemoryTableOperationOutcomeTask m_operationOutcomeTask;
    SequenceNumber m_initialWriteSequenceNumber;

    MemoryTableOperationOutcomeTask GetOperationOutcome()
    {
        co_return co_await m_outcomeValueSource.wait();
    }

public:
    Operation(
        ProtoStore& protoStore,
        SequenceNumber initialWriteSequenceNumber
    )
    :
        m_protoStore(protoStore),
        m_logRecord(make_unique<LogRecord>()),
        m_initialWriteSequenceNumber(initialWriteSequenceNumber)
    {
        m_operationOutcomeTask = GetOperationOutcome();
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
        SequenceNumber readSequenceNumber, 
        ProtoIndex protoIndex,
        const ProtoValue& key,
        const ProtoValue& value
    ) override
    {
        auto index = protoIndex.m_index;

        auto checkpointNumber = co_await index->AddRow(
            readSequenceNumber,
            key,
            value,
            m_initialWriteSequenceNumber,
            m_operationOutcomeTask
        );

        auto loggedRowWrite = m_logRecord->add_rows();
        loggedRowWrite->set_sequencenumber(ToUint64(m_initialWriteSequenceNumber));
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
        ReadRequest& readRequest
    ) override
    {
        return m_protoStore.Read(
            readRequest);
    }

    // Inherited via IOperationTransaction
    virtual task<CommitResult> Commit(
    ) override
    {
        MemoryTableOperationOutcome outcome =
        {
            .Outcome = OperationOutcome::Committed,
            .WriteSequenceNumber = m_initialWriteSequenceNumber,
        };

        m_outcomeValueSource.emplace(
            outcome);

        co_await m_protoStore.WriteLogRecord(
            *m_logRecord
        );

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
    auto previousWriteSequenceNumber = m_nextWriteSequenceNumber.fetch_add(
        1,
        std::memory_order_acq_rel);
    auto thisWriteSequenceNumber = previousWriteSequenceNumber + 1;

    auto executionOperationTask = ExecuteOperation(
        visitor,
        thisWriteSequenceNumber);

    //auto publishTask = Publish(
    //    executionOperationTask,
    //    previousWriteSequenceNumber,
    //    thisWriteSequenceNumber);

    //m_asyncScope.spawn(move(
    //    publishTask));

    co_return co_await executionOperationTask;
}

shared_task<OperationResult> ProtoStore::ExecuteOperation(
    OperationVisitor visitor,
    uint64_t thisWriteSequenceNumber
)
{
    Operation operation(
        *this,
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

    auto readResult = co_await m_indexesByNameIndex->Read(
        readRequest);

    if (readResult.ReadStatus == ReadStatus::NoValue)
    {
        co_return nullptr;
    }

    auto indexesByNameValue = readResult.Value.cast_if<IndexesByNameValue>();

    co_return co_await GetIndexInternal(
        indexesByNameValue->indexnumber());
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

    co_await ExecuteOperation(
        BeginTransactionRequest(),
        [&](IOperation* operation)->task<>
    {
        WriteOperationMetadata metadata;

        co_await operation->AddRow(
            metadata,
            SequenceNumber::Earliest,
            ProtoIndex(this, &*m_indexesByNumberIndex),
            &indexesByNumberKey,
            &indexesByNumberValue
        );

        IndexesByNameKey indexesByNameKey;
        IndexesByNameValue indexesByNameValue;

        indexesByNameKey.set_indexname(
            indexesByNumberValue.indexname());
        indexesByNameValue.set_indexnumber(
            indexNumber);

        co_await operation->AddRow(
            metadata,
            SequenceNumber::Earliest,
            ProtoIndex(this, &*m_indexesByNameIndex),
            &indexesByNameKey,
            &indexesByNameValue
        );

    });

    auto index = co_await GetIndexInternal(
        indexNumber);

    co_return ProtoIndex(
        this,
        &*index);
}

task<shared_ptr<IIndex>> ProtoStore::GetIndexInternal(
    google::protobuf::uint64 indexNumber
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
    auto readResult = co_await m_indexesByNumberIndex->Read(
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

        auto index = MakeIndex(
            indexesByNumberKey,
            *indexesByNumberValue);

        m_indexesByNumber[indexNumber] = index;

        co_return index;
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

shared_ptr<IIndex> ProtoStore::MakeIndex(
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

    return index;
}

task<shared_ptr<IPartition>> ProtoStore::OpenPartitionForIndex(
    const shared_ptr<IIndex>& index,
    ExtentNumber dataExtentNumber,
    ExtentNumber headerExtentNumber)
{
    if (m_activePartitions.contains(dataExtentNumber))
    {
        co_return m_activePartitions[dataExtentNumber];
    }

    auto partition = make_shared<Partition>(
        index->GetKeyComparer(),
        index->GetKeyFactory(),
        index->GetValueFactory(),
        m_dataHeaderMessageAccessor,
        m_dataMessageAccessor,
        ExtentLocation
        {
            .extentNumber = headerExtentNumber,
            .extentOffset = 0,
        },
        ExtentLocation
        {
            .extentNumber = dataExtentNumber,
            .extentOffset = 0,
        });

    co_await partition->Open();

    m_activePartitions[dataExtentNumber] = partition;

    co_return partition;
}

shared_task<> ProtoStore::WaitForCheckpoints(
    optional<shared_task<>> previousCheckpoint,
    shared_task<> newCheckpoint
)
{
    if (previousCheckpoint.has_value())
    {
        co_await *previousCheckpoint;
    }
    co_await newCheckpoint;
}

task<> ProtoStore::Checkpoint()
{
    shared_task newAllCheckpointsTask;

    {
        auto lock = co_await m_checkpointTaskLock.scoped_lock_async();
        auto newCheckpointTask = InternalCheckpoint();
        newAllCheckpointsTask = WaitForCheckpoints(
            m_previousCheckpoints,
            newCheckpointTask);
        m_previousCheckpoints = newAllCheckpointsTask;
    }

    co_await newAllCheckpointsTask;
}

shared_task<> ProtoStore::InternalCheckpoint()
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
    shared_ptr<IIndex> index
)
{
    auto loggedCheckpoint = co_await index->StartCheckpoint();

    if (!loggedCheckpoint.checkpointnumber_size())
    {
        co_return;
    }

    auto dataExtentNumber = co_await AllocateDataExtent();

    auto dataWriter = co_await m_dataMessageStore->OpenExtentForSequentialWriteAccess(
        dataExtentNumber);
    auto headerWriter = co_await m_dataHeaderMessageStore->OpenExtentForSequentialWriteAccess(
        dataExtentNumber);

    PartitionWriterParameters partitionWriterParameters{};

    auto partitionWriter = make_shared<PartitionWriter>(
        partitionWriterParameters,
        dataWriter,
        headerWriter);

    co_await index->Checkpoint(
        loggedCheckpoint,
        partitionWriter);

    auto writeSequenceNumber = ToSequenceNumber(
        m_nextWriteSequenceNumber.fetch_add(1));

    Operation operation(
        *this,
        writeSequenceNumber);

    operation.m_logRecord->mutable_extras()->add_loggedactions()->mutable_loggedcheckpoints()->CopyFrom(
        loggedCheckpoint);
    operation.m_logRecord->mutable_extras()->add_loggedactions()->mutable_loggedcommitdataextents()->set_extentnumber(
        dataExtentNumber);

    PartitionsKey partitionsKey;
    partitionsKey.set_indexnumber(index->GetIndexNumber());
    partitionsKey.set_dataextentnumber(dataExtentNumber);
    PartitionsValue partitionsValue;
    partitionsValue.set_headerextentnumber(dataExtentNumber);
    partitionsValue.set_size(co_await dataWriter->CurrentOffset());
    partitionsValue.set_level(0);

    {
        auto updatePartitionsMutex = co_await m_updatePartitionsMutex.scoped_lock_async();

        co_await operation.AddRow(
            WriteOperationMetadata(),
            writeSequenceNumber,
            ProtoIndex{ &*this, &*m_partitionsIndex },
            &partitionsKey,
            &partitionsValue);

        co_await operation.Commit();

        auto partitions = co_await OpenPartitionsForIndex(
            index);

        co_await index->UpdatePartitions(
            loggedCheckpoint,
            partitions);
    }
}

task<vector<shared_ptr<IPartition>>> ProtoStore::OpenPartitionsForIndex(
    const shared_ptr<IIndex>& index)
{
    PartitionsKey partitionsKeyLow;
    partitionsKeyLow.set_indexnumber(index->GetIndexNumber());
    partitionsKeyLow.set_dataextentnumber(0);

    PartitionsKey partitionsKeyHigh;
    partitionsKeyHigh.set_indexnumber(index->GetIndexNumber() + 1);
    partitionsKeyHigh.set_dataextentnumber(0);

    EnumerateRequest enumerateRequest;
    enumerateRequest.KeyLow = &partitionsKeyLow;
    enumerateRequest.KeyLowInclusivity = Inclusivity::Inclusive;
    enumerateRequest.KeyHigh = &partitionsKeyHigh;
    enumerateRequest.KeyHighInclusivity = Inclusivity::Exclusive;
    enumerateRequest.SequenceNumber = SequenceNumber::LatestCommitted;
    enumerateRequest.Index = ProtoIndex{ this, &*m_partitionsIndex, };

    auto enumeration = m_partitionsIndex->Enumerate(
        enumerateRequest);

    vector<shared_ptr<IPartition>> partitions;

    for (auto iterator = co_await enumeration.begin();
        iterator != enumeration.end();
        co_await ++iterator)
    {
        auto partitionsKey = (*iterator).Key.cast_if<PartitionsKey>();
        auto partitionsValue = (*iterator).Value.cast_if<PartitionsValue>();

        assert(partitionsKey);
        assert(partitionsValue);

        auto partition = co_await OpenPartitionForIndex(
            index,
            partitionsKey->dataextentnumber(),
            partitionsValue->headerextentnumber());

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
            index.second->Join());
    }

    co_await cppcoro::when_all(
        move(joinTasks));

    co_await AsyncScopeMixin::Join();
}

task<ExtentNumber> ProtoStore::AllocateDataExtent()
{
    auto extentNumber = m_nextDataExtentNumber.fetch_add(1);
    LogRecord logRecord;
    logRecord.mutable_extras()
        ->add_loggedactions()
        ->mutable_loggedcreatedataextents()
        ->set_extentnumber(extentNumber);

    co_await WriteLogRecord(
        logRecord);

    co_return extentNumber;
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

}