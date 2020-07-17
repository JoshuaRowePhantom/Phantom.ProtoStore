#include "StandardIncludes.h"

#include "Phantom.ProtoStore/src/MemoryExtentStore.h"
#include "ProtoStoreTest.pb.h"
#include <cppcoro/single_consumer_event.hpp>
#include <cppcoro/when_all_ready.hpp>

namespace Phantom::ProtoStore
{

task<shared_ptr<IProtoStore>> CreateMemoryStore()
{
    auto storeFactory = MakeProtoStoreFactory();
    CreateProtoStoreRequest createRequest;

    createRequest.HeaderExtentStore = UseMemoryExtentStore();
    createRequest.LogExtentStore = UseMemoryExtentStore();
    createRequest.DataExtentStore = UseMemoryExtentStore();

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

TEST(ProtoStoreTests, Can_read_written_row_during_operation)
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

TEST(ProtoStoreTests, Can_commit_transaction)
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

}
