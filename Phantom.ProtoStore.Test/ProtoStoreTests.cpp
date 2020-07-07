#include "StandardIncludes.h"

#include "Phantom.ProtoStore/src/MemoryExtentStore.h"
#include "ProtoStoreTest.pb.h"

namespace Phantom::ProtoStore
{

task<shared_ptr<IProtoStore>> CreateMemoryStore()
{
    auto storeFactory = MakeProtoStoreFactory();
    CreateProtoStoreRequest createRequest;

    createRequest.ExtentStore = UseMemoryExtentStore();

    co_return co_await storeFactory->Create(
        createRequest);
}

TEST(ProtoStoreTests, CanCreate_memory_backed_store)
{
    run_async([]() -> task<>
    {
        auto store = CreateMemoryStore();
        co_return;
    });
}

TEST(ProtoStoreTests, CanOpen_memory_backed_store)
{
    run_async([]() -> task<>
    {
        auto storeFactory = MakeProtoStoreFactory();
        CreateProtoStoreRequest createRequest;

        createRequest.ExtentStore = UseMemoryExtentStore();

        auto store = co_await storeFactory->Create(
            createRequest);
        store.reset();

        store = co_await storeFactory->Open(
            createRequest);
    });
}

TEST(ProtoStoreTests, Open_fails_on_uncreated_store)
{
    run_async([]() -> task<>
    {
        auto storeFactory = MakeProtoStoreFactory();
        OpenProtoStoreRequest openRequest;

        openRequest.ExtentStore = UseMemoryExtentStore();

        ASSERT_THROW(
            co_await storeFactory->Open(
                openRequest),
            range_error);
    });
}

TEST(ProtoStoreTests, Can_read_and_write_one_row)
{
    run_async([]() -> task<>
    {
        auto store = co_await CreateMemoryStore();

        ProtoIndex index;
        StringKey key;
        key.set_value("testKey1");
        StringValue expectedValue;
        expectedValue.set_value("testValue1");

        auto outcome = co_await store->ExecuteOperation(
            BeginTransactionRequest(),
            [&](IOperation* operation)->task<OperationOutcome>
        {
            CreateIndexRequest createIndexRequest;
            createIndexRequest.IndexName = "test_Index";
            createIndexRequest.KeySchema.KeyDescriptor = StringKey::descriptor();
            createIndexRequest.ValueSchema.ValueDescriptor = StringValue::descriptor();

            index = co_await operation->CreateIndex(
                WriteOperationMetadata(),
                createIndexRequest
            );

            co_await operation->AddRow(
                WriteOperationMetadata(),
                SequenceNumber::Latest,
                &key,
                &expectedValue);

            co_return OperationOutcome::Committed;
        });

        ASSERT_EQ(
            OperationOutcome::Committed,
            outcome);

        ReadRequest readRequest;
        readRequest.Key = &key;
        readRequest.Index = index;

        auto readResult = co_await store->Read(
            readRequest
        );

        StringValue actualValue;
        readResult.Value.unpack(&actualValue);
    });
}
}
