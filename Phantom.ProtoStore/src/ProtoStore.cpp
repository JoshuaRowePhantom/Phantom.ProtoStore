#include "ProtoStore.h"
#include "StandardTypes.h"
#include "MessageStore.h"
#include "RandomMessageAccessor.h"
#include "HeaderAccessor.h"
#include "Schema.h"
#include "src/ProtoStoreInternal.pb.h"
#include "IndexImpl.h"
#include "Phantom.System/async_value_source.h"
#include <cppcoro/sync_wait.hpp>
#include <cppcoro/when_all.hpp>
#include <cppcoro/on_scope_exit.hpp>

namespace Phantom::ProtoStore
{

ProtoStore::ProtoStore(
    shared_ptr<IExtentStore> headerExtentStore,
    shared_ptr<IExtentStore> logExtentStore,
    shared_ptr<IExtentStore> dataExtentStore)
    :
    m_headerExtentStore(move(headerExtentStore)),
    m_logExtentStore(move(logExtentStore)),
    m_dataExtentStore(move(dataExtentStore)),
    m_headerMessageStore(MakeMessageStore(m_headerExtentStore)),
    m_logMessageStore(MakeMessageStore(m_logExtentStore)),
    m_dataMessageStore(MakeMessageStore(m_dataExtentStore)),
    m_headerMessageAccessor(MakeRandomMessageAccessor(m_headerMessageStore)),
    m_dataMessageAccessor(MakeRandomMessageAccessor(m_dataMessageStore)),
    m_headerAccessor(MakeHeaderAccessor(m_headerMessageAccessor))
{
    m_writeSequenceNumberBarrier.publish(0);
}

task<> ProtoStore::Create(
    const CreateProtoStoreRequest& createRequest)
{
    // Write out the log record that initializes a database.
    {
        auto logMessageWriter = co_await m_logMessageStore->OpenExtentForSequentialWriteAccess(
            0);

        LogRecord logRecord;

        NextIndexNumberKey nextIndexNumberKey;
        NextIndexNumberValue nextIndexNumberValue;
        nextIndexNumberValue.set_nextindexnumber(1000);
        auto nextIndexNumberRow = logRecord.add_rows();
        nextIndexNumberRow->set_indexnumber(2);
        nextIndexNumberRow->set_sequencenumber(0);
        nextIndexNumberKey.AppendToString(
            nextIndexNumberRow->mutable_key());
        nextIndexNumberValue.AppendToString(
            nextIndexNumberRow->mutable_value());

        co_await logMessageWriter->Write(
            logRecord,
            FlushBehavior::Flush);
    }

    Header header;
    header.set_version(1);
    header.set_epoch(1);
    header.set_logalignment(static_cast<google::protobuf::uint32>(
        createRequest.LogAlignment));
    header.add_logreplayextentnumbers(0);

    co_await m_headerAccessor->WriteHeader(
        header);

    co_await Open(
        createRequest);
}

task<> ProtoStore::Open(
    const OpenProtoStoreRequest& openRequest)
{
    Header header;
    co_await m_headerAccessor->ReadHeader(
        header);

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
            "__System.NextIndexNumber",
            2,
            SequenceNumber::Earliest,
            NextIndexNumberKey::descriptor(),
            NextIndexNumberValue::descriptor());

        m_nextIndexNumberIndex = MakeIndex(
            indexesByNumberKey,
            indexesByNumberValue
        );
    }

    m_indexesByNumber[m_indexesByNumberIndex->GetIndexNumber()] = m_indexesByNumberIndex;
    m_indexesByNumber[m_indexesByNameIndex->GetIndexNumber()] = m_indexesByNameIndex;
    m_indexesByNumber[m_nextIndexNumberIndex->GetIndexNumber()] = m_nextIndexNumberIndex;

    for (auto logReplayExtentNumber : header.logreplayextentnumbers())
    {
        co_await Replay(
            logReplayExtentNumber);
    }

    co_await OpenLogWriter();

    co_return;
}

task<> ProtoStore::OpenLogWriter()
{
    auto logExtentNumber = 0;

    co_await UpdateHeader([&](Header& header)->task<>
    {
        std::set<ExtentNumber> usedExtents;
        for (auto usedExtent : header.logreplayextentnumbers())
        {
            usedExtents.insert(usedExtent);
        }

        while (usedExtents.contains(logExtentNumber))
        {
            logExtentNumber++;
        }

        header.add_logreplayextentnumbers(logExtentNumber);

        co_return;
    });

    m_logWriter = co_await m_logMessageStore->OpenExtentForSequentialWriteAccess(
        logExtentNumber);
}

task<> ProtoStore::UpdateHeader(
    std::function<task<>(Header&)> modifier
)
{
    auto lock = co_await m_headerMutex.scoped_lock_async();
    
    Header header;
    
    co_await m_headerAccessor->ReadHeader(
        header);
    
    auto nextEpoch = header.epoch() + 1;
    
    co_await modifier(
        header);

    header.set_epoch(
        nextEpoch);

    co_await m_headerAccessor->WriteHeader(
        header);
}

task<> ProtoStore::WriteLogRecord(
    const LogRecord& logRecord
)
{
    co_await m_logWriter->Write(
        logRecord,
        FlushBehavior::Flush
    );
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
    for (auto loggedRowWrite : logRecord.rows())
    {
        auto index = co_await GetIndexInternal(
            loggedRowWrite.indexnumber());

        co_await index->Replay(
            loggedRowWrite);
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
    ProtoStore& m_protoStore;
    unique_ptr<LogRecord> m_logRecord;
    async_value_source<MemoryTableOperationOutcome> m_outcomeValueSource;
    MemoryTableOperationOutcomeTask m_operationOutcomeTask;
    SequenceNumber m_initialWriteSequenceNumber;

    MemoryTableOperationOutcomeTask GetOperationOutcome()
    {
        co_return co_await m_outcomeValueSource;
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

        auto loggedRowWrite = m_logRecord->add_rows();
        loggedRowWrite->set_sequencenumber(ToUint64(m_initialWriteSequenceNumber));
        loggedRowWrite->set_indexnumber(protoIndex.m_index->GetIndexNumber());
        key.pack(
            loggedRowWrite->mutable_key());
        value.pack(
            loggedRowWrite->mutable_value());

        co_await index->AddRow(
            readSequenceNumber,
            key,
            value,
            m_initialWriteSequenceNumber,
            m_operationOutcomeTask
        );
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

    auto executionOperationTask = [&]() -> shared_task<OperationResult>
    {
        Operation operation(
            *this,
            ToSequenceNumber(thisWriteSequenceNumber));

        co_await visitor(&operation);
        co_await operation.Commit();

        co_return OperationResult{};
    }();

    auto publishTask = [=]() -> task<>
    {
        co_await executionOperationTask.when_ready();

        co_await m_writeSequenceNumberBarrier.wait_until_published(
            previousWriteSequenceNumber,
            m_inlineScheduler);

        m_writeSequenceNumberBarrier.publish(
            thisWriteSequenceNumber);
    }();

    m_asyncScope.spawn(move(
        publishTask));

    co_return co_await executionOperationTask;
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
    IndexNumber allocatedIndexNumber;

    while(true)
    {
        try
        {
            co_await ExecuteOperation(
                BeginTransactionRequest(),
                [&](IOperation* operation)->task<>
            {
                NextIndexNumberKey nextIndexNumberKey;

                ReadRequest readRequest;
                readRequest.SequenceNumber = SequenceNumber::Latest;
                readRequest.Index = ProtoIndex(this, &*m_nextIndexNumberIndex);
                readRequest.Key = &nextIndexNumberKey;
                readRequest.ReadValueDisposition = ReadValueDisposition::ReadValue;

                auto readResult = co_await operation->Read(
                    readRequest);

                auto nextIndexNumberValue = readResult.Value.cast_if<NextIndexNumberValue>();

                allocatedIndexNumber = nextIndexNumberValue->nextindexnumber();

                NextIndexNumberValue newNextIndexNumberValue;
                newNextIndexNumberValue.set_nextindexnumber(allocatedIndexNumber + 1);

                WriteOperationMetadata writeOperationMetadata;
                co_await operation->AddRow(
                    writeOperationMetadata,
                    readResult.WriteSequenceNumber,
                    ProtoIndex(this, &*m_nextIndexNumberIndex),
                    &nextIndexNumberKey,
                    &newNextIndexNumberValue);
            });

            co_return allocatedIndexNumber;
        }
        catch (WriteConflict)
        {
            continue;
        }
    }
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
        auto lock = co_await m_indexesByNumberLock.scoped_nonrecursive_lock_read_async();
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
        auto lock = co_await m_indexesByNumberLock.scoped_nonrecursive_lock_write_async();
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

}