#include "StandardIncludes.h"

#include "Phantom.ProtoStore/src/MemoryExtentStore.h"
#include "ProtoStoreTest.pb.h"
#include <cppcoro/single_consumer_event.hpp>
#include <cppcoro/async_scope.hpp>
#include <cppcoro/static_thread_pool.hpp>
#include <cppcoro/when_all_ready.hpp>

#include <vector>
using namespace std;

namespace Phantom::ProtoStore
{

CreateProtoStoreRequest GetCreateMemoryStoreRequest()
{
    CreateProtoStoreRequest createRequest;

    createRequest.HeaderExtentStore = UseMemoryExtentStore();
    createRequest.LogExtentStore = UseMemoryExtentStore();
    createRequest.DataExtentStore = UseMemoryExtentStore();

    return createRequest;
}

task<shared_ptr<IProtoStore>> CreateStore(
    const CreateProtoStoreRequest& createRequest)
{
    auto storeFactory = MakeProtoStoreFactory();

    co_return co_await storeFactory->Create(
        createRequest);
}

task<shared_ptr<IProtoStore>> CreateMemoryStore()
{
    co_return co_await CreateStore(
        GetCreateMemoryStoreRequest());
}

task<shared_ptr<IProtoStore>> OpenStore(
    const OpenProtoStoreRequest& request
)
{
    auto storeFactory = MakeProtoStoreFactory();

    co_return co_await storeFactory->Open(
        request);
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

        createRequest.HeaderExtentStore = UseMemoryExtentStore();
        createRequest.LogExtentStore = UseMemoryExtentStore();
        createRequest.DataExtentStore = UseMemoryExtentStore();

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

        openRequest.HeaderExtentStore = UseMemoryExtentStore();
        openRequest.LogExtentStore = UseMemoryExtentStore();
        openRequest.DataExtentStore = UseMemoryExtentStore();

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

        CreateIndexRequest createIndexRequest;
        createIndexRequest.IndexName = "test_Index";
        createIndexRequest.KeySchema.KeyDescriptor = StringKey::descriptor();
        createIndexRequest.ValueSchema.ValueDescriptor = StringValue::descriptor();

        index = co_await store->CreateIndex(
            createIndexRequest
        );

        co_await store->ExecuteOperation(
            BeginTransactionRequest(),
            [&](IOperation* operation)->task<>
        {
            co_await operation->AddRow(
                WriteOperationMetadata(),
                SequenceNumber::Latest,
                index,
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

        ASSERT_TRUE(MessageDifferencer::Equals(
            expectedValue,
            actualValue));
    });
}

TEST(ProtoStoreTests, Can_read_and_write_one_row_after_reopen)
{
    run_async([]() -> task<>
    {
        auto createRequest = GetCreateMemoryStoreRequest();

        auto store = co_await CreateStore(createRequest);

        ProtoIndex index;
        StringKey key;
        key.set_value("testKey1");
        StringValue expectedValue;
        expectedValue.set_value("testValue1");

        CreateIndexRequest createIndexRequest;
        createIndexRequest.IndexName = "test_Index";
        createIndexRequest.KeySchema.KeyDescriptor = StringKey::descriptor();
        createIndexRequest.ValueSchema.ValueDescriptor = StringValue::descriptor();

        index = co_await store->CreateIndex(
            createIndexRequest
        );

        co_await store->ExecuteOperation(
            BeginTransactionRequest(),
            [&](IOperation* operation)->task<>
        {
            co_await operation->AddRow(
                WriteOperationMetadata(),
                SequenceNumber::Latest,
                index,
                &key,
                &expectedValue);
        });

        store.reset();
        store = co_await OpenStore(createRequest);

        index = co_await store->GetIndex(
            createIndexRequest);

        ReadRequest readRequest;
        readRequest.Key = &key;
        readRequest.Index = index;

        auto readResult = co_await store->Read(
            readRequest
        );

        StringValue actualValue;
        readResult.Value.unpack(&actualValue);

        ASSERT_TRUE(MessageDifferencer::Equals(
            expectedValue,
            actualValue));
    });
}

TEST(ProtoStoreTests, Can_read_and_write_one_row_after_checkpoint)
{
    run_async([]() -> task<>
    {
        auto createRequest = GetCreateMemoryStoreRequest();

        auto store = co_await CreateStore(createRequest);

        ProtoIndex index;
        StringKey key;
        key.set_value("testKey1");
        StringValue expectedValue;
        expectedValue.set_value("testValue1");

        CreateIndexRequest createIndexRequest;
        createIndexRequest.IndexName = "test_Index";
        createIndexRequest.KeySchema.KeyDescriptor = StringKey::descriptor();
        createIndexRequest.ValueSchema.ValueDescriptor = StringValue::descriptor();

        index = co_await store->CreateIndex(
            createIndexRequest
        );

        co_await store->ExecuteOperation(
            BeginTransactionRequest(),
            [&](IOperation* operation)->task<>
        {
            co_await operation->AddRow(
                WriteOperationMetadata(),
                SequenceNumber::Latest,
                index,
                &key,
                &expectedValue);
        });

        co_await store->Checkpoint();

        ReadRequest readRequest;
        readRequest.Key = &key;
        readRequest.Index = index;

        auto readResult = co_await store->Read(
            readRequest
        );

        StringValue actualValue;
        readResult.Value.unpack(&actualValue);

        ASSERT_TRUE(MessageDifferencer::Equals(
            expectedValue,
            actualValue));
    });
}

TEST(ProtoStoreTests, DISABLED_Can_read_written_row_during_operation)
{
    run_async([]() -> task<>
    {
        auto store = co_await CreateMemoryStore();

        ProtoIndex index;
        StringKey key;
        key.set_value("testKey1");
        StringValue expectedValue;
        expectedValue.set_value("testValue1");

        CreateIndexRequest createIndexRequest;
        createIndexRequest.IndexName = "test_Index";
        createIndexRequest.KeySchema.KeyDescriptor = StringKey::descriptor();
        createIndexRequest.ValueSchema.ValueDescriptor = StringValue::descriptor();

        index = co_await store->CreateIndex(
            createIndexRequest
        );

        co_await store->ExecuteOperation(
            BeginTransactionRequest(),
            [&](IOperation* operation)->task<>
        {
            co_await operation->AddRow(
                WriteOperationMetadata(),
                SequenceNumber::Latest,
                index,
                &key,
                &expectedValue);

            {
                ReadRequest readRequest;
                readRequest.Key = &key;
                readRequest.Index = index;

                auto readResult = co_await store->Read(
                    readRequest
                );

                StringValue actualValue;
                readResult.Value.unpack(&actualValue);
            }
        });

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

        CreateIndexRequest createIndexRequest;
        createIndexRequest.IndexName = "test_Index";
        createIndexRequest.KeySchema.KeyDescriptor = StringKey::descriptor();
        createIndexRequest.ValueSchema.ValueDescriptor = StringValue::descriptor();

        index = co_await store->CreateIndex(
            createIndexRequest
        );

        cppcoro::single_consumer_event addRowEvent;

        auto operation1 = store->ExecuteOperation(
            BeginTransactionRequest(),
            [&](IOperation* operation)->task<>
        {
            co_await operation->AddRow(
                WriteOperationMetadata(),
                SequenceNumber::Latest,
                index,
                &key,
                &expectedValue);

            addRowEvent.set();
        });

        auto operation2 = store->ExecuteOperation(
            BeginTransactionRequest(),
            [&](IOperation* operation)->task<>
        {
            co_await addRowEvent;
            co_await operation->AddRow(
                WriteOperationMetadata(),
                SequenceNumber::Earliest,
                index,
                &key,
                &unexpectedValue);
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

TEST(ProtoStoreTests, DISABLED_Can_commit_transaction)
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
        TransactionId transactionId("transactionId1");

        CreateIndexRequest createIndexRequest;
        createIndexRequest.IndexName = "test_Index";
        createIndexRequest.KeySchema.KeyDescriptor = StringKey::descriptor();
        createIndexRequest.ValueSchema.ValueDescriptor = StringValue::descriptor();

        index = co_await store->CreateIndex(
            createIndexRequest
        );

        co_await store->ExecuteOperation(
            BeginTransactionRequest(),
            [&](IOperation* operation)->task<>
        {
            co_await operation->AddRow(
                WriteOperationMetadata
            {
                .TransactionId = &transactionId,
            },
                SequenceNumber::Latest,
                index,
                &key,
                &expectedValue);
        });

        {
            ReadRequest readRequest;
            readRequest.Key = &key;
            readRequest.Index = index;

            ASSERT_THROW(
                co_await store->Read(
                    readRequest),
                UnresolvedTransactionConflict);
        }

        co_await store->ExecuteOperation(
            BeginTransactionRequest(),
            [&](IOperation* operation)->task<>
        {
            co_await operation->ResolveTransaction(
                WriteOperationMetadata
                {
                    .TransactionId = &transactionId,
                },
                TransactionOutcome::Committed);
        });

        {
            ReadRequest readRequest;
            readRequest.Key = &key;
            readRequest.Index = index;

            auto readResult = co_await store->Read(
                readRequest
            );

            StringValue actualValue;
            readResult.Value.unpack(&actualValue);
        }
    });
}

TEST(ProtoStoreTests, Perf1)
{
    run_async([=]() -> task<>
    {
        auto store = co_await CreateMemoryStore();
        cppcoro::static_thread_pool threadPool(4);

        CreateIndexRequest createIndexRequest;
        createIndexRequest.IndexName = "test_Index";
        createIndexRequest.KeySchema.KeyDescriptor = StringKey::descriptor();
        createIndexRequest.ValueSchema.ValueDescriptor = StringValue::descriptor();

        auto index = co_await store->CreateIndex(
            createIndexRequest
        );

        int valueCount = 100;

        mt19937 rng;
        uniform_int_distribution<int> distribution('a', 'z');

        auto keys = MakeRandomStrings(
            rng,
            20,
            valueCount
        );

        auto startTime = chrono::high_resolution_clock::now();

        cppcoro::async_scope asyncScope;

        for (auto value : keys)
        {
            asyncScope.spawn([&](string myKey) -> task<>
            {
                co_await threadPool.schedule();

                StringKey key;
                key.set_value(myKey);
                StringValue expectedValue;
                expectedValue.set_value(myKey);

                co_await store->ExecuteOperation(
                    BeginTransactionRequest(),
                    [&](IOperation* operation)->task<>
                {
                    co_await operation->AddRow(
                        WriteOperationMetadata{},
                        SequenceNumber::Latest,
                        index,
                        &key,
                        &expectedValue);
                });
            }(value));
        }

        co_await asyncScope.join();

        auto endTime = chrono::high_resolution_clock::now();

        auto runtimeMs = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);

        std::cout << "ProtoStoreTests runtime: " << runtimeMs.count() << "\r\n";
    });
}

}
