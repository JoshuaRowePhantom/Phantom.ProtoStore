#include "TestFactories.h"
#include "Phantom.ProtoStore/src/Schema.h"
#include "Phantom.System/utility.h"

namespace Phantom::ProtoStore
{

std::atomic<uint64_t> testLocalTransactionId;
std::atomic<uint64_t> testWriteId;

task<std::shared_ptr<IIndexData>> MakeInMemoryIndex(
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


task<OperationResult<>> AddRow(
    const std::shared_ptr<IIndexData>& index,
    ProtoValue key,
    ProtoValue value,
    SequenceNumber writeSequenceNumber,
    SequenceNumber readSequenceNumber
)
{
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
            ToUint64(writeSequenceNumber),
            checkpointNumber,
            keyOffset,
            valueOffset,
            0,
            testLocalTransactionId.fetch_add(1),
            testWriteId.fetch_add(1)
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
        writeSequenceNumber);

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

}

