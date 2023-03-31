#include "TestFactories.h"
#include "Phantom.ProtoStore/src/Schema.h"
#include "Phantom.System/utility.h"
#include <flatbuffers/idl.h>

namespace Phantom::ProtoStore
{

task<std::shared_ptr<IIndexData>> TestFactories::MakeInMemoryIndex(
    IndexName indexName,
    const Schema& schema
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
        indexName,
        indexNumber,
        ToSequenceNumber(0),
        keyComparer,
        valueComparer,
        nullptr,
        schemaPtr
    );

    auto memoryTable = MakeMemoryTable(
        schemaPtr,
        keyComparer);

    co_await index->SetDataSources(
        memoryTable,
        0,
        {},
        {}
    );

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

    auto createLoggedRowWrite = [&](auto checkpointNumber) -> task<FlatMessage<FlatBuffers::LoggedRowWrite>>
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
            checkpointNumber,
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

    auto checkpointNumber = co_await index->AddRow(
        readSequenceNumber,
        createLoggedRowWrite,
        delayedTransactionOutcome
    );

    if (!checkpointNumber)
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
    bool ok =
        parser.Deserialize(schema)
        && parser.SetRootType(object->name()->c_str())
        && parser.ParseJson(json->c_str());
    assert(ok);
    return ProtoValue::FlatBuffer(
        std::move(parser.builder_)
    );
}

}

