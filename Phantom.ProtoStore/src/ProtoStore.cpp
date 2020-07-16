#include "ProtoStore.h"
#include "StandardTypes.h"
#include "MessageStore.h"
#include "RandomMessageAccessor.h"
#include "HeaderAccessor.h"
#include "Schema.h"
#include "src/ProtoStoreInternal.pb.h"
#include "IndexImpl.h"

namespace Phantom::ProtoStore
{

ProtoStore::ProtoStore(
    shared_ptr<IExtentStore> extentStore)
    :
    m_extentStore(move(extentStore)),
    m_messageStore(MakeMessageStore(m_extentStore)),
    m_messageAccessor(MakeRandomMessageAccessor(m_messageStore)),
    m_headerAccessor(MakeHeaderAccessor(m_messageAccessor))
{
}

task<> ProtoStore::Create(
    const CreateProtoStoreRequest& createRequest)
{
    Header header;
    header.set_version(1);
    header.set_epoch(1);
    header.set_logalignment(static_cast<google::protobuf::uint32>(
        createRequest.LogAlignment));
    header.set_logreplayextentnumber(2);
    header.set_logreplaylocation(0);

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
    throw 0;
}

class Operation
    :
    public IOperationTransaction
{
    ProtoStore& m_protoStore;
public:
    Operation(
        ProtoStore& protoStore)
    :
        m_protoStore(protoStore)
    {}

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
        const ProtoValue& key, 
        const ProtoValue& value
    ) override
    {
        return task<>();
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
        return task<ProtoIndex>();
    }

    virtual task<ReadResult> Read(
        ReadRequest& readRequest
    ) override
    {
        return task<ReadResult>();
    }

    // Inherited via IOperationTransaction
    virtual task<CommitResult> Commit(
    ) override
    {
        return task<CommitResult>();
    }
};

task<OperationResult> ProtoStore::ExecuteOperation(
    const BeginTransactionRequest beginRequest,
    OperationVisitor visitor
)
{
    Operation operation(
        *this);
    co_await visitor(&operation);
    co_await operation.Commit();
    co_return OperationResult{};
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
    while(true)
    {
        try
        {
            NextIndexNumberKey nextIndexNumberKey;

            ReadRequest readRequest;
            readRequest.Key = &nextIndexNumberKey;
            readRequest.ReadValueDisposition = ReadValueDisposition::ReadValue;

            auto readResult = co_await m_nextIndexNumberIndex->Read(
                readRequest);

            auto nextIndexNumberValue = readResult.Value.cast_if<NextIndexNumberValue>();

            auto allocatedIndexNumber = nextIndexNumberValue->nextindexnumber();

            NextIndexNumberValue newNextIndexNumberValue;
            newNextIndexNumberValue.set_nextindexnumber(allocatedIndexNumber + 1);

            Operation operation(
                *this);

            WriteOperationMetadata writeOperationMetadata;
            co_await operation.AddRow(
                writeOperationMetadata,
                readResult.WriteSequenceNumber,
                &nextIndexNumberKey,
                &newNextIndexNumberValue
            );

            co_await operation.Commit();

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

    WriteOperationMetadata metadata;

    Operation operation(*this);
    co_await operation.AddRow(
        metadata,
        SequenceNumber::Earliest,
        &indexesByNumberKey,
        &indexesByNumberValue);

    IndexesByNameKey indexesByNameKey;
    IndexesByNameValue indexesByNameValue;

    indexesByNameKey.set_indexname(
        indexesByNumberValue.indexname());
    indexesByNameValue.set_indexnumber(
        indexNumber);

    co_await operation.AddRow(
        metadata,
        SequenceNumber::Earliest,
        &indexesByNameKey,
        &indexesByNameValue);

    co_await operation.Commit();

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
    co_return;
}

}