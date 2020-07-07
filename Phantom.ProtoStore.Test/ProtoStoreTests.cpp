#include "StandardIncludes.h"

#include "Phantom.ProtoStore/src/MemoryExtentStore.h"
#include "ProtoStoreTest.pb.h"
#include <cppcoro/async_latch.hpp>
#include <cppcoro/when_all_ready.hpp>

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

        co_await store->ExecuteOperation(
            BeginTransactionRequest(),
            [&](IOperation* operation)->task<>
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
        });

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

TEST(ProtoStoreTests, Can_conflict_on_one_row_and_commits_first)
{
    run_async([]() -> task<>
    {
        auto store = co_await CreateMemoryStore();

        ProtoIndex index;
        StringKey key;
        key.set_value("testKey1");
        StringValue expectedValue;
        expectedValue.set_value("testValue1");
        StringValue unexpectedValue;
        unexpectedValue.set_value("testValue2");

        co_await store->ExecuteOperation(
            BeginTransactionRequest(),
            [&](IOperation* operation)->task<>
        {
            CreateIndexRequest createIndexRequest;
            createIndexRequest.IndexName = "test_Index";
            createIndexRequest.KeySchema.KeyDescriptor = StringKey::descriptor();
            createIndexRequest.ValueSchema.ValueDescriptor = StringValue::descriptor();

            index = co_await operation->CreateIndex(
                WriteOperationMetadata(),
                createIndexRequest
            );
        });

        cppcoro::async_latch addRowLatch(1);
        cppcoro::async_latch continueLatch(1);

        auto operation1 = store->ExecuteOperation(
            BeginTransactionRequest(),
            [&](IOperation* operation)->task<>
        {
            co_await operation->AddRow(
                WriteOperationMetadata(),
                SequenceNumber::Latest,
                &key,
                &expectedValue);

            addRowLatch.count_down();
            co_await continueLatch;
        });

        auto operation2 = store->ExecuteOperation(
            BeginTransactionRequest(),
            [&](IOperation* operation)->task<>
        {
            co_await addRowLatch;
            co_await operation->AddRow(
                WriteOperationMetadata(),
                SequenceNumber::Latest,
                &key,
                &unexpectedValue);
            continueLatch.count_down();
        });

        auto result = co_await cppcoro::when_all_ready(
            move(operation1),
            move(operation2));

        ASSERT_NO_THROW(get<0>(result).result());
        ASSERT_THROW(get<1>(result).result(), WriteConflict);

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
