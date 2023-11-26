#include "TestFactories.h"
#include "Phantom.ProtoStore/src/IndexDataSourcesImpl.h"
#include "Phantom.ProtoStore/src/MemoryExtentStore.h"
#include "Phantom.ProtoStore/src/MessageStore.h"
#include "Phantom.ProtoStore/src/PartitionImpl.h"
#include "Phantom.ProtoStore/src/PartitionWriterImpl.h"
#include "Phantom.ProtoStore/src/ProtoStore.h"
#include "Phantom.ProtoStore/src/Schema.h"
#include "Phantom.System/utility.h"
#include <flatbuffers/idl.h>

namespace Phantom::ProtoStore
{

IUnresolvedTransactionsTracker* TestAccessors::GetUnresolvedTransactionsTracker(
    const ProtoStore* protoStore)
{
    return protoStore->m_unresolvedTransactionsTracker.get();
}

task<std::shared_ptr<IIndexData>> TestFactories::MakeInMemoryIndex(
    IndexName indexName,
    const Schema& schema,
    FlatValue<FlatBuffers::Metadata> metadata
)
{
    static std::atomic<IndexNumber> nextIndexNumber = 10000;

    auto schemaPtr = copy_shared(schema);
    auto indexNumber = nextIndexNumber.fetch_add(1);

    auto keyComparer = SchemaDescriptions::MakeKeyComparer(
        schemaPtr);
    auto valueComparer = SchemaDescriptions::MakeValueComparer(
        schemaPtr);

    auto index = MakeIndex(
        Schedulers::Inline(),
        indexName,
        indexNumber,
        ToSequenceNumber(0),
        keyComparer,
        valueComparer,
        nullptr,
        schemaPtr,
        std::move(metadata)
    );

    auto memoryTable = MakeMemoryTable(
        schemaPtr,
        keyComparer);

    co_await index->SetDataSources(
        std::make_shared<IndexDataSourcesSelector>(
            memoryTable,
            1000,
            std::vector<std::shared_ptr<IMemoryTable>>(),
            std::vector<std::shared_ptr<IPartition>>()
    ));

    co_return index;
}

task<OperationResult<>> TestFactories::AddRow(
    const std::shared_ptr<IIndexData>& index,
    ProtoValue key,
    ProtoValue value,
    std::optional<SequenceNumber> writeSequenceNumber,
    SequenceNumber readSequenceNumber
)
{
    if (!writeSequenceNumber)
    {
        writeSequenceNumber = ToSequenceNumber(m_nextWriteSequenceNumber.fetch_add(1));
    }

    auto createLoggedRowWrite = [&](auto partitionNumber) -> task<FlatMessage<FlatBuffers::LoggedRowWrite>>
    {
        ValueBuilder valueBuilder;

        auto keyOffset = index->GetKeyComparer()->BuildDataValue(
            valueBuilder,
            key);

        auto valueOffset = index->GetValueComparer()->BuildDataValue(
            valueBuilder,
            value);

        auto loggedRowWriteOffset = FlatBuffers::CreateLoggedRowWrite(
            valueBuilder.builder(),
            index->GetIndexNumber(),
            ToUint64(*writeSequenceNumber),
            partitionNumber,
            keyOffset,
            valueOffset,
            0,
            m_nextTestLocalTransactionId.fetch_add(1),
            m_nextTestWriteId.fetch_add(1)
        );

        valueBuilder.builder().Finish(loggedRowWriteOffset);

        co_return FlatMessage<FlatBuffers::LoggedRowWrite>(
            std::make_shared<flatbuffers::FlatBufferBuilder>(std::move(valueBuilder.builder())));
    };

    auto delayedTransactionOutcome = std::make_shared<DelayedMemoryTableTransactionOutcome>(
        0
    );

    auto partitionNumber = co_await index->AddRow(
        readSequenceNumber,
        createLoggedRowWrite,
        delayedTransactionOutcome
    );

    if (!partitionNumber)
    {
        co_return std::unexpected
        {
            FailedResult
            {
                make_error_code(ProtoStoreErrorCode::WriteConflict),
            }
        };
    }

    auto outcome = delayedTransactionOutcome->BeginCommit(
        *writeSequenceNumber);

    delayedTransactionOutcome->Complete();

    if (outcome.Outcome == TransactionOutcome::Committed)
    {
        co_return{};
    }

    co_return std::unexpected
    {
        FailedResult
        {
            make_error_code(ProtoStoreErrorCode::AbortedTransaction),
        }
    };
}

ProtoValue TestFactories::JsonToProtoValue(
    const reflection::Schema* schema,
    const reflection::Object* object,
    std::optional<string> json)
{
    if (!json)
    {
        return ProtoValue{};
    }

    flatbuffers::Parser parser;
    parser.opts.json_nested_legacy_flatbuffers = true;
    bool ok = parser.Deserialize(schema);
    assert(ok);
    ok = parser.SetRootType(object->name()->c_str());
    assert(ok);
    ok = parser.ParseJson(json->c_str());
    assert(ok);
    return ProtoValue::FlatBuffer(
        std::move(parser.builder_)
    );
}

std::string TestFactories::JsonToJsonBytes(
    const reflection::Schema* schema,
    const reflection::Object* object,
    const std::string& json
)
{
    auto protoValue = JsonToProtoValue(
        schema,
        object,
        json);

    auto bytes = protoValue.as_flat_buffer_bytes_if();

    std::ostringstream stream;
    stream << "[";
    const char* separator = "";
    for (auto byte : bytes)
    {
        stream << separator << (int)byte;
        separator = ",";
    }
    stream << "]";

    return stream.str();
}

TestFactories::TestPartitionBuilder::TestPartitionBuilder(
    shared_ptr<IMessageStore> messageStore
) :
    m_messageStore(messageStore)
{
}

task<> TestFactories::TestPartitionBuilder::OpenForWrite(
    IndexNumber indexNumber,
    PartitionNumber partitionNumber,
    LevelNumber levelNumber,
    std::string indexName
)
{
    m_headerExtentName = MakePartitionHeaderExtentName(
        indexNumber,
        partitionNumber,
        levelNumber,
        indexName);

    auto headerExtentNameFlatMessage = FlatMessage{ m_headerExtentName };

    m_dataExtentName = MakePartitionDataExtentName(
        headerExtentNameFlatMessage->extent_name_as_IndexHeaderExtentName());

    auto dataExtentNameFlatMessage = FlatMessage{ m_dataExtentName };

    m_headerWriter = co_await m_messageStore->OpenExtentForSequentialWriteAccess(
        headerExtentNameFlatMessage.get()
    );

    m_dataWriter = co_await m_messageStore->OpenExtentForSequentialWriteAccess(
        dataExtentNameFlatMessage.get()
    );
}

task<FlatBuffers::MessageReference_V1> TestFactories::TestPartitionBuilder::Write(
    const shared_ptr<ISequentialMessageWriter>& writer,
    const std::string& json
)
{
    auto protoValue = JsonToProtoValue(
        FlatBuffersSchemas().ProtoStoreInternalSchema,
        FlatBuffersSchemas().PartitionMessage_Object,
        json
    );

    FlatMessage<FlatBuffers::PartitionMessage> flatMessage(
        32,
        protoValue.as_flat_buffer_bytes_if()
    );

    auto storedMessage = co_await writer->Write(
        flatMessage.data(),
        FlushBehavior::Flush
    );

    auto header = storedMessage->Header_V1();

    FlatBuffers::MessageReference_V1 messageReference =
    {
        *header,
        storedMessage->DataRange.Beginning,
    };

    co_return messageReference;
}

task<FlatBuffers::MessageReference_V1> TestFactories::TestPartitionBuilder::WriteData(
    const std::string& json
)
{
    co_return co_await Write(
        m_dataWriter,
        json);
}

task<FlatBuffers::MessageReference_V1> TestFactories::TestPartitionBuilder::WriteHeader(
    const std::string& json
)
{
    co_return co_await Write(
        m_headerWriter,
        json);
}

ExtentNameT TestFactories::TestPartitionBuilder::HeaderExtentName() const
{
    return m_headerExtentName;
}

ExtentNameT TestFactories::TestPartitionBuilder::DataExtentName() const
{
    return m_dataExtentName;
}

task<shared_ptr<IPartition>> TestFactories::TestPartitionBuilder::OpenPartition(
    const Schema& schema)
{
    auto headerExtentNameFlatMessage = FlatMessage{ m_headerExtentName };

    auto dataExtentNameFlatMessage = FlatMessage{ m_dataExtentName };

    auto headerReader = co_await m_messageStore->OpenExtentForRandomReadAccess(
        headerExtentNameFlatMessage.get()
    );

    auto dataReader = co_await m_messageStore->OpenExtentForRandomReadAccess(
        dataExtentNameFlatMessage.get()
    );

    auto sharedSchema = copy_shared(schema);
    
    auto keyComparer = SchemaDescriptions::MakeKeyComparer(
        sharedSchema);

    auto partition = std::make_shared<Partition>(
        sharedSchema,
        keyComparer,
        std::move(headerReader),
        std::move(dataReader)
    );

    co_await partition->Open();

    co_return partition;
}


shared_ptr<ProtoStore> TestFactories::ToProtoStore(
    shared_ptr<IProtoStore> protoStore
)
{
    return std::static_pointer_cast<ProtoStore>(
        protoStore
    );
}


task<shared_ptr<IPartition>> TestFactories::CreateInMemoryTestPartition(
    std::vector<TestStringKeyValuePairRow> rows
)
{
    auto extentStore = co_await UseMemoryExtentStore()();
    auto messageStore = MakeMessageStore(
        Schedulers::Inline(),
        extentStore);

    auto headerExtentName = FlatValue{ MakePartitionHeaderExtentName(1, 1, 1, "test") };
    auto dataExtentName = FlatValue{ MakePartitionHeaderExtentName(1, 1, 1, "test") };

    auto headerWriter = co_await messageStore->OpenExtentForSequentialWriteAccess(
        headerExtentName
    );
    auto dataWriter = co_await messageStore->OpenExtentForSequentialWriteAccess(
        dataExtentName
    );
    auto schema = copy_shared(Schema::Make(
        { GetSchema<FlatBuffers::FlatStringKey>(), GetObject<FlatBuffers::FlatStringKey>() },
        { GetSchema<FlatBuffers::FlatStringValue>(), GetObject<FlatBuffers::FlatStringValue>() }
    ));

    auto keyComparer = MakeFlatBufferValueComparer(copy_shared(schema->KeySchema.AsFlatBuffersKeySchema()->ObjectSchema));
    auto valueComparer = MakeFlatBufferValueComparer(copy_shared(schema->ValueSchema.AsFlatBuffersValueSchema()->ObjectSchema));

    auto partitionWriter = PartitionWriter(
        schema,
        keyComparer,
        valueComparer,
        headerWriter,
        dataWriter
    );

    auto rowGeneratorLambda = [&]() -> row_generator
    {
        for (auto& row : rows)
        {
            FlatBuffers::FlatStringKeyT keyT;
            keyT.value = row.Key;
            ProtoValue key = FlatMessage{ keyT };
            ProtoValue value;
            if (row.Value)
            {
                FlatBuffers::FlatStringValueT valueT;
                valueT.value = *row.Value;
                value = FlatMessage{ valueT };
            }
            TransactionIdReference transactionIdReference;
            if (row.DistributedTransactionId)
            {
                FlatBuffers::FlatStringKeyT transactionIdHolderT;
                transactionIdHolderT.value = *row.DistributedTransactionId;
                auto transactionIdHolder = FlatMessage{ transactionIdHolderT };
                transactionIdReference = MakeTransactionIdReference(
                    transactionIdHolder,
                    transactionIdHolder->value());
            }

            co_yield ResultRow
            {
                .Key = key,
                .WriteSequenceNumber = row.WriteSequenceNumber,
                .Value = value,
                .TransactionId = transactionIdReference,
            };
        }
    };

    auto rowGenerator = rowGeneratorLambda();

    WriteRowsRequest writeRowsRequest
    {
        .approximateRowCount = rows.size(),
        .rows = &rowGenerator,
        .inputSize = rows.size() * 1000,
        .targetExtentSize = std::numeric_limits<size_t>::max(),
        .targetMessageSize = std::numeric_limits<size_t>::max(),
    };

    auto writeRowsResult = co_await partitionWriter.WriteRows(
        writeRowsRequest
    );

    auto headerReader = co_await messageStore->OpenExtentForRandomReadAccess(
        headerExtentName
    );

    auto dataReader = co_await messageStore->OpenExtentForRandomReadAccess(
        dataExtentName
    );

    auto partition = std::make_shared<Partition>(
        schema,
        keyComparer,
        headerReader,
        dataReader
    );

    co_await partition->Open();

    co_return partition;
}

task<shared_ptr<IMemoryTable>> TestFactories::CreateTestMemoryTable(
    std::vector<TestStringKeyValuePairRow> rows
)
{
    throw 1;
}

}

