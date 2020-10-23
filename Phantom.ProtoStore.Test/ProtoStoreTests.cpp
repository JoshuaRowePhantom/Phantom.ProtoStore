#include "StandardIncludes.h"

#include "Phantom.ProtoStore/src/MemoryExtentStore.h"
#include "ProtoStoreTest.pb.h"
#include <cppcoro/single_consumer_event.hpp>
#include <cppcoro/async_scope.hpp>
#include <cppcoro/static_thread_pool.hpp>
#include <cppcoro/when_all.hpp>
#include <cppcoro/when_all_ready.hpp>

using namespace std;

namespace Phantom::ProtoStore
{

class ProtoStoreTests
    :
    public testing::Test
{
public:
    CreateProtoStoreRequest GetCreateMemoryStoreRequest()
    {
        CreateProtoStoreRequest createRequest;
        
        createRequest.ExtentStore = UseMemoryExtentStore();

        return createRequest;
    }

    CreateProtoStoreRequest GetCreateFileStoreRequest(
        string testName)
    {
        CreateProtoStoreRequest createRequest;
        createRequest.ExtentStore = UseFilesystemStore(testName, "test", 4096);
        createRequest.Schedulers = Schedulers::Inline();

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

    task<ProtoIndex> CreateTestIndex(
        const shared_ptr<IProtoStore>& store
    )
    {
        CreateIndexRequest createIndexRequest;
        createIndexRequest.IndexName = "test_Index";
        createIndexRequest.KeySchema.KeyDescriptor = StringKey::descriptor();
        createIndexRequest.ValueSchema.ValueDescriptor = StringValue::descriptor();

        auto index = co_await store->CreateIndex(
            createIndexRequest
        );
        co_return index;
    }

    task<ProtoIndex> GetTestIndex(
        const shared_ptr<IProtoStore>& store
    )
    {
        CreateIndexRequest createIndexRequest;
        createIndexRequest.IndexName = "test_Index";
        createIndexRequest.KeySchema.KeyDescriptor = StringKey::descriptor();
        createIndexRequest.ValueSchema.ValueDescriptor = StringValue::descriptor();

        auto index = co_await store->GetIndex(
            createIndexRequest
        );
        co_return index;
    }

    task<> AddRowToTestIndex(
        const shared_ptr<IProtoStore>& store,
        ProtoIndex index,
        string key,
        optional<string> value,
        SequenceNumber writeSequenceNumber,
        optional<SequenceNumber> readSequenceNumber = optional<SequenceNumber>()
    )
    {
        StringKey stringKey;
        stringKey.set_value(key);
        StringValue stringValue;
        if (value.has_value())
        {
            stringValue.set_value(*value);
        }

        co_await store->ExecuteOperation(
            BeginTransactionRequest(),
            [&](IOperation* operation)->task<>
        {
            co_await operation->AddRow(
                WriteOperationMetadata
                {
                    .ReadSequenceNumber = readSequenceNumber,
                    .WriteSequenceNumber = writeSequenceNumber,
                },
                index,
                &stringKey,
                value.has_value() ? & stringValue : nullptr);
        });
    }

    task<> ExpectGetTestRow(
        const shared_ptr<IProtoStore>& store,
        ProtoIndex index,
        string key,
        optional<string> expectedValue,
        SequenceNumber readSequenceNumber,
        SequenceNumber expectedSequenceNumber
    )
    {
        StringKey stringKey;
        stringKey.set_value(key);

        ReadRequest readRequest;
        readRequest.Key = &stringKey;
        readRequest.Index = index;
        readRequest.SequenceNumber = readSequenceNumber;
        
        auto readResult = co_await store->Read(
            readRequest
        );

        if (!expectedValue.has_value())
        {
            EXPECT_EQ(false, readResult.Value.has_value());
        }
        else
        {
            StringValue actualValue;
            readResult.Value.unpack(&actualValue);
            EXPECT_EQ(*expectedValue, actualValue.value());
            EXPECT_EQ(expectedSequenceNumber, readResult.WriteSequenceNumber);
        }
    }

    task<vector<row<StringKey, StringValue>>> EnumerateTestRows(
        const shared_ptr<IProtoStore>& store,
        ProtoIndex index,
        optional<string> keyLow,
        Inclusivity keyLowInclusivity,
        optional<string> keyHigh,
        Inclusivity keyHighInclusivity,
        SequenceNumber readSequenceNumber
    )
    {
        StringKey keyLowStringKey;
        StringKey keyHighStringKey;
        if (keyLow)
        {
            keyLowStringKey.set_value(*keyLow);
        }
        if (keyHigh)
        {
            keyHighStringKey.set_value(*keyHigh);
        }

        EnumerateRequest enumerateRequest;
        enumerateRequest.KeyLow = keyLow.has_value() ? ProtoValue(&keyLowStringKey) : ProtoValue::KeyMin();
        enumerateRequest.KeyLowInclusivity = keyLowInclusivity;
        enumerateRequest.KeyHigh = keyHigh.has_value() ? ProtoValue(&keyHighStringKey) : ProtoValue::KeyMax();
        enumerateRequest.KeyHighInclusivity = keyHighInclusivity;
        enumerateRequest.SequenceNumber = readSequenceNumber;
        enumerateRequest.Index = index;

        auto enumeration = store->Enumerate(
            enumerateRequest);

        vector<row<StringKey, StringValue>> result;

        for (auto iterator = co_await enumeration.begin();
            iterator != enumeration.end();
            co_await ++iterator)
        {
            row<StringKey, StringValue> resultRow;
            (*iterator).Key.unpack(&resultRow.Key);
            (*iterator).Value.unpack(&resultRow.Value);
            resultRow.ReadSequenceNumber = (*iterator).WriteSequenceNumber;
            resultRow.WriteSequenceNumber = (*iterator).WriteSequenceNumber;
            result.push_back(resultRow);
        }

        co_return result;
    }

    task<> AssertEnumeration(
        const shared_ptr<IProtoStore>& store,
        ProtoIndex index,
        optional<string> keyLow,
        Inclusivity keyLowInclusivity,
        optional<string> keyHigh,
        Inclusivity keyHighInclusivity,
        SequenceNumber readSequenceNumber,
        map<string, tuple<string, uint64_t>> expectedRows
    )
    {
        auto actualRows = co_await EnumerateTestRows(
            store,
            index,
            keyLow,
            keyLowInclusivity,
            keyHigh,
            keyHighInclusivity,
            readSequenceNumber);

        for (auto& actualRow : actualRows)
        {
            EXPECT_TRUE(expectedRows.contains(actualRow.Key.value()));
            EXPECT_EQ(get<0>(expectedRows[actualRow.Key.value()]), actualRow.Value.value());
            EXPECT_EQ(ToSequenceNumber(get<1>(expectedRows[actualRow.Key.value()])), actualRow.WriteSequenceNumber);

            expectedRows.erase(actualRow.Key.value());
        }

        EXPECT_TRUE(expectedRows.empty());
    }
};

TEST_F(ProtoStoreTests, CanCreate_memory_backed_store)
{
    run_async([&]() -> task<>
    {
        auto store = CreateMemoryStore();
        co_return;
    });
}

TEST_F(ProtoStoreTests, CanOpen_memory_backed_store)
{
    run_async([&]() -> task<>
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

TEST_F(ProtoStoreTests, Open_fails_on_uncreated_store)
{
    run_async([&]() -> task<>
    {
        auto storeFactory = MakeProtoStoreFactory();
        OpenProtoStoreRequest openRequest;

        openRequest.ExtentStore = UseMemoryExtentStore();

        EXPECT_THROW(
            co_await storeFactory->Open(
                openRequest),
            range_error);
    });
}

TEST_F(ProtoStoreTests, Can_read_and_write_one_row)
{
    run_async([&]() -> task<>
    {
        auto store = co_await CreateMemoryStore();

        auto index = co_await CreateTestIndex(
            store);

        co_await AddRowToTestIndex(
            store,
            index,
            "testKey1",
            "testValue1",
            ToSequenceNumber(5));

        co_await ExpectGetTestRow(
            store,
            index,
            "testKey1",
            "testValue1",
            ToSequenceNumber(5),
            ToSequenceNumber(5));
    });
}

TEST_F(ProtoStoreTests, Can_read_and_delete_and_enumerate_one_row)
{
    run_async([&]() -> task<>
    {
        auto store = co_await CreateMemoryStore();

        auto index = co_await CreateTestIndex(
            store);

        co_await AddRowToTestIndex(
            store,
            index,
            "testKey1",
            "testValue1",
            ToSequenceNumber(5));

        co_await AddRowToTestIndex(
            store,
            index,
            "testKey1",
            optional<string>(),
            ToSequenceNumber(6),
            ToSequenceNumber(5));

        auto enumerateAtSequenceNumber5 = co_await EnumerateTestRows(
            store,
            index,
            optional<string>(),
            Inclusivity::Inclusive,
            optional<string>(),
            Inclusivity::Inclusive,
            ToSequenceNumber(5));

        auto enumerateAtSequenceNumber6 = co_await EnumerateTestRows(
            store,
            index,
            optional<string>(),
            Inclusivity::Inclusive,
            optional<string>(),
            Inclusivity::Inclusive,
            ToSequenceNumber(6));


        co_await AssertEnumeration(
            store,
            index,
            optional<string>(),
            Inclusivity::Inclusive,
            optional<string>(),
            Inclusivity::Inclusive,
            ToSequenceNumber(5),
            {
                { "testKey1", {"testValue1", 5}},
            });


        co_await AssertEnumeration(
            store,
            index,
            optional<string>(),
            Inclusivity::Inclusive,
            optional<string>(),
            Inclusivity::Inclusive,
            ToSequenceNumber(6),
            {
            });
    });
}

TEST_F(ProtoStoreTests, Can_enumerate_one_row_after_add)
{
    run_async([&]() -> task<>
    {
        auto store = co_await CreateMemoryStore();

        auto index = co_await CreateTestIndex(
            store);

        co_await AddRowToTestIndex(
            store,
            index,
            "testKey1",
            "testValue1",
            ToSequenceNumber(5));

        co_await AssertEnumeration(
            store,
            index,
            optional<string>(),
            Inclusivity::Inclusive,
            optional<string>(),
            Inclusivity::Inclusive,
            ToSequenceNumber(5),
            {
                { "testKey1", {"testValue1", 5}},
            });
    });
}

TEST_F(ProtoStoreTests, Can_enumerate_one_row_after_checkpoint)
{
    run_async([&]() -> task<>
    {
        auto store = co_await CreateMemoryStore();

        auto index = co_await CreateTestIndex(
            store);

        co_await AddRowToTestIndex(
            store,
            index,
            "testKey1",
            "testValue1",
            ToSequenceNumber(5));

        co_await store->Checkpoint();

        co_await AssertEnumeration(
            store,
            index,
            optional<string>(),
            Inclusivity::Inclusive,
            optional<string>(),
            Inclusivity::Inclusive,
            ToSequenceNumber(5),
            {
                { "testKey1", {"testValue1", 5}},
            });
    });
}

TEST_F(ProtoStoreTests, Can_enumerate_one_row_after_update)
{
    run_async([&]() -> task<>
    {
        auto store = co_await CreateMemoryStore();

        auto index = co_await CreateTestIndex(
            store);

        co_await AddRowToTestIndex(
            store,
            index,
            "testKey1",
            "testValue1",
            ToSequenceNumber(5));

        co_await AddRowToTestIndex(
            store,
            index,
            "testKey1",
            "testValue1-2",
            ToSequenceNumber(6),
            ToSequenceNumber(5));

        co_await AssertEnumeration(
            store,
            index,
            optional<string>(),
            Inclusivity::Inclusive,
            optional<string>(),
            Inclusivity::Inclusive,
            ToSequenceNumber(5),
            {
                { "testKey1", {"testValue1", 5}},
            });

        co_await AssertEnumeration(
            store,
            index,
            optional<string>(),
            Inclusivity::Inclusive,
            optional<string>(),
            Inclusivity::Inclusive,
            ToSequenceNumber(6),
            {
                { "testKey1", {"testValue1-2", 6}},
            });
    });
}

TEST_F(ProtoStoreTests, Can_enumerate_one_row_after_checkpoint_and_update)
{
    run_async([&]() -> task<>
    {
        auto store = co_await CreateMemoryStore();

        auto index = co_await CreateTestIndex(
            store);

        co_await AddRowToTestIndex(
            store,
            index,
            "testKey1",
            "testValue1",
            ToSequenceNumber(5));

        co_await store->Checkpoint();

        co_await AddRowToTestIndex(
            store,
            index,
            "testKey1",
            "testValue1-2",
            ToSequenceNumber(6),
            ToSequenceNumber(5));

        co_await AssertEnumeration(
            store,
            index,
            optional<string>(),
            Inclusivity::Inclusive,
            optional<string>(),
            Inclusivity::Inclusive,
            ToSequenceNumber(5),
            {
                { "testKey1", {"testValue1", 5}},
            });

        co_await AssertEnumeration(
            store,
            index,
            optional<string>(),
            Inclusivity::Inclusive,
            optional<string>(),
            Inclusivity::Inclusive,
            ToSequenceNumber(6),
            {
                { "testKey1", {"testValue1-2", 6}},
            });
    });
}

TEST_F(ProtoStoreTests, Can_enumerate_one_row_after_two_checkpoint_and_update_and_checkpoint)
{
    run_async([&]() -> task<>
    {
        auto store = co_await CreateMemoryStore();

        auto index = co_await CreateTestIndex(
            store);

        co_await AddRowToTestIndex(
            store,
            index,
            "testKey1",
            "testValue1",
            ToSequenceNumber(5));

        co_await store->Checkpoint();

        co_await AddRowToTestIndex(
            store,
            index,
            "testKey1",
            "testValue1-2",
            ToSequenceNumber(6),
            ToSequenceNumber(5));

        co_await store->Checkpoint();

        co_await AssertEnumeration(
            store,
            index,
            optional<string>(),
            Inclusivity::Inclusive,
            optional<string>(),
            Inclusivity::Inclusive,
            ToSequenceNumber(5),
            {
                { "testKey1", {"testValue1", 5}},
            });

        co_await AssertEnumeration(
            store,
            index,
            optional<string>(),
            Inclusivity::Inclusive,
            optional<string>(),
            Inclusivity::Inclusive,
            ToSequenceNumber(6),
            {
                { "testKey1", {"testValue1-2", 6}},
            });
    });
}

TEST_F(ProtoStoreTests, Can_read_and_write_rows_after_checkpoints_and_merges)
{
    run_async([&]() -> task<>
    {
        auto createRequest = GetCreateFileStoreRequest("Can_read_and_write_rows_after_checkpoints_and_merges");
        createRequest.DefaultMergeParameters.set_mergesperlevel(2);
        createRequest.DefaultMergeParameters.set_maxlevel(4);
        //auto createRequest = GetCreateMemoryStoreRequest();
        auto store = co_await CreateStore(createRequest);

        auto index = co_await CreateTestIndex(
            store);

        ranlux48 rng;
        auto keys = MakeRandomStrings(
            rng,
            20,
            100);

        for (auto key : keys)
        {
            co_await AddRowToTestIndex(
                store,
                index,
                key,
                key + "-value",
                ToSequenceNumber(5));

            co_await store->Checkpoint();
            co_await store->Merge();
        }

        for (auto key : keys)
        {
            co_await ExpectGetTestRow(
                store,
                index,
                key,
                key + "-value",
                ToSequenceNumber(5),
                ToSequenceNumber(5));
        }

        store.reset();
        store = co_await OpenStore(createRequest);

        index = co_await GetTestIndex(
            store);

        for (auto key : keys)
        {
            co_await ExpectGetTestRow(
                store,
                index,
                key,
                key + "-value",
                ToSequenceNumber(5),
                ToSequenceNumber(5));
        }    
    });
}

TEST_F(ProtoStoreTests, Can_read_and_write_one_row_multiple_versions)
{
    run_async([&]() -> task<>
    {
        auto store = co_await CreateMemoryStore();

        auto index = co_await CreateTestIndex(
            store);

        co_await AddRowToTestIndex(
            store,
            index,
            "testKey1",
            "testValue1-5",
            ToSequenceNumber(5));

        co_await AddRowToTestIndex(
            store,
            index,
            "testKey1",
            "testValue1-6",
            ToSequenceNumber(6),
            ToSequenceNumber(5));

        co_await ExpectGetTestRow(
            store,
            index,
            "testKey1",
            optional<string>(),
            ToSequenceNumber(4),
            ToSequenceNumber(5));

        co_await ExpectGetTestRow(
            store,
            index,
            "testKey1",
            "testValue1-5",
            ToSequenceNumber(5),
            ToSequenceNumber(5));

        co_await ExpectGetTestRow(
            store,
            index,
            "testKey1",
            "testValue1-6",
            ToSequenceNumber(6),
            ToSequenceNumber(6)); 

        co_await ExpectGetTestRow(
            store,
            index,
            "testKey1",
            "testValue1-6",
            ToSequenceNumber(7),
            ToSequenceNumber(6)); 
    });
}

TEST_F(ProtoStoreTests, Can_read_and_write_one_row_multiple_versions_after_checkpoints)
{
    run_async([&]() -> task<>
    {
        auto store = co_await CreateMemoryStore();

        auto index = co_await CreateTestIndex(
            store);

        co_await AddRowToTestIndex(
            store,
            index,
            "testKey1",
            "testValue1-5",
            ToSequenceNumber(5));

        co_await store->Checkpoint();

        co_await AddRowToTestIndex(
            store,
            index,
            "testKey1",
            "testValue1-6",
            ToSequenceNumber(6),
            ToSequenceNumber(5));

        co_await store->Checkpoint();

        co_await AddRowToTestIndex(
            store,
            index,
            "testKey1",
            "testValue1-7",
            ToSequenceNumber(7),
            ToSequenceNumber(6));

        co_await ExpectGetTestRow(
            store,
            index,
            "testKey1",
            optional<string>(),
            ToSequenceNumber(4),
            ToSequenceNumber(5));

        co_await ExpectGetTestRow(
            store,
            index,
            "testKey1",
            "testValue1-5",
            ToSequenceNumber(5),
            ToSequenceNumber(5));

        co_await ExpectGetTestRow(
            store,
            index,
            "testKey1",
            "testValue1-6",
            ToSequenceNumber(6),
            ToSequenceNumber(6));

        co_await ExpectGetTestRow(
            store,
            index,
            "testKey1",
            "testValue1-7",
            ToSequenceNumber(7),
            ToSequenceNumber(7));

        co_await ExpectGetTestRow(
            store,
            index,
            "testKey1",
            "testValue1-7",
            ToSequenceNumber(8),
            ToSequenceNumber(7));
    });
}

TEST_F(ProtoStoreTests, Can_conflict_after_row_written)
{
    run_async([&]() -> task<>
    {
        auto store = co_await CreateMemoryStore();

        auto index = co_await CreateTestIndex(
            store);

        co_await AddRowToTestIndex(
            store,
            index,
            "testKey1",
            "testValue1",
            ToSequenceNumber(5));

        EXPECT_THROW(
            co_await AddRowToTestIndex(
                store,
                index,
                "testKey1",
                "testValue2",
                ToSequenceNumber(4)),
            WriteConflict);

        co_await ExpectGetTestRow(
            store,
            index,
            "testKey1",
            "testValue1",
            ToSequenceNumber(5),
            ToSequenceNumber(5));

        co_await ExpectGetTestRow(
            store,
            index,
            "testKey1",
            optional<string>(),
            ToSequenceNumber(4),
            ToSequenceNumber(5));    
    });
}

TEST_F(ProtoStoreTests, Can_conflict_after_row_checkpointed)
{
    run_async([&]() -> task<>
    {
        auto store = co_await CreateMemoryStore();

        auto index = co_await CreateTestIndex(
            store);

        co_await AddRowToTestIndex(
            store,
            index,
            "testKey1",
            "testValue1",
            ToSequenceNumber(5));

        co_await store->Checkpoint();

        EXPECT_THROW(
            co_await AddRowToTestIndex(
                store,
                index,
                "testKey1",
                "testValue2",
                ToSequenceNumber(4)),
            WriteConflict);

        co_await ExpectGetTestRow(
            store,
            index,
            "testKey1",
            "testValue1",
            ToSequenceNumber(5),
            ToSequenceNumber(5));

        co_await ExpectGetTestRow(
            store,
            index,
            "testKey1",
            optional<string>(),
            ToSequenceNumber(4),
            ToSequenceNumber(5));
    });
}

TEST_F(ProtoStoreTests, Can_read_and_write_one_row_after_reopen)
{
    run_async([&]() -> task<>
    {
        auto createRequest = GetCreateMemoryStoreRequest();

        auto store = co_await CreateStore(createRequest);

        auto index = co_await CreateTestIndex(
            store);

        co_await AddRowToTestIndex(
            store,
            index,
            "testKey1",
            "testValue1",
            ToSequenceNumber(5));

        store.reset();
        store = co_await OpenStore(createRequest);

        index = co_await GetTestIndex(
            store);

        co_await ExpectGetTestRow(
            store,
            index,
            "testKey1",
            "testValue1",
            ToSequenceNumber(5),
            ToSequenceNumber(5));
    });
}

TEST_F(ProtoStoreTests, Checkpoint_deletes_old_logs)
{
    run_async([&]() -> task<>
    {
        //auto createRequest = GetCreateFileStoreRequest("Checkpoint_deletes_old_logs");
        auto createRequest = GetCreateMemoryStoreRequest();

        auto store = co_await CreateStore(createRequest);

        auto memoryStore = std::static_pointer_cast<MemoryExtentStore>(
            co_await createRequest.ExtentStore());

        auto index = co_await CreateTestIndex(
            store);

        co_await AddRowToTestIndex(
            store,
            index,
            "testKey1",
            "testValue1",
            ToSequenceNumber(5));

        // Checkpoint twice to ensure old log is delete.
        co_await store->Checkpoint();
        EXPECT_EQ(true, co_await memoryStore->ExtentExists(MakeLogExtentName(0)));

        co_await store->Checkpoint();

        EXPECT_EQ(false, co_await memoryStore->ExtentExists(MakeLogExtentName(0)));
    });
}

TEST_F(ProtoStoreTests, Can_read_and_write_one_row_after_checkpoint)
{
    run_async([&]() -> task<>
    {
        auto createRequest = GetCreateMemoryStoreRequest();

        auto store = co_await CreateStore(createRequest);

        auto index = co_await CreateTestIndex(
            store);

        co_await AddRowToTestIndex(
            store,
            index,
            "testKey1",
            "testValue1",
            ToSequenceNumber(5));

        co_await store->Checkpoint();

        co_await ExpectGetTestRow(
            store,
            index,
            "testKey1",
            "testValue1",
            ToSequenceNumber(5),
            ToSequenceNumber(5));
    });
}

TEST_F(ProtoStoreTests, Can_read_and_write_one_row_after_checkpoint_and_reopen)
{
    run_async([&]() -> task<>
    {
        auto createRequest = GetCreateFileStoreRequest("Can_read_and_write_one_row_after_checkpoint_and_reopen");

        auto store = co_await CreateStore(createRequest);

        auto index = co_await CreateTestIndex(
            store);

        co_await AddRowToTestIndex(
            store,
            index,
            "testKey1",
            "testValue1",
            ToSequenceNumber(5));

        // Checkpoint a few times to make sure old log files are deleted.
        co_await store->Checkpoint();
        co_await store->Checkpoint();
        co_await store->Checkpoint();
        co_await store->Join();
        store.reset();
        store = co_await OpenStore(createRequest);

        index = co_await GetTestIndex(
            store);

        co_await ExpectGetTestRow(
            store,
            index,
            "testKey1",
            "testValue1",
            ToSequenceNumber(5),
            ToSequenceNumber(5));
    });
}

TEST_F(ProtoStoreTests, DISABLED_Can_read_written_row_during_operation)
{
    run_async([&]() -> task<>
    {
        auto store = co_await CreateMemoryStore();

        StringKey key;
        key.set_value("testKey1");
        StringValue expectedValue;
        expectedValue.set_value("testValue1");

        auto index = co_await CreateTestIndex(
            store);

        co_await store->ExecuteOperation(
            BeginTransactionRequest(),
            [&](IOperation* operation)->task<>
        {
            co_await operation->AddRow(
                WriteOperationMetadata(),
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

TEST_F(ProtoStoreTests, Can_conflict_on_one_row_and_commits_first)
{
    run_async([&]() -> task<>
    {
        auto store = co_await CreateMemoryStore();

        StringKey key;
        key.set_value("testKey1");
        StringValue expectedValue;
        expectedValue.set_value("testValue1");
        StringValue unexpectedValue;
        unexpectedValue.set_value("testValue2");

        auto index = co_await CreateTestIndex(
            store);

        cppcoro::single_consumer_event addRowEvent;

        auto operation1 = store->ExecuteOperation(
            BeginTransactionRequest(),
            [&](IOperation* operation)->task<>
        {
            co_await operation->AddRow(
                WriteOperationMetadata
                {
                    .WriteSequenceNumber = ToSequenceNumber(5),
                },
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
                WriteOperationMetadata
                {
                    .WriteSequenceNumber = ToSequenceNumber(4),
                },
                index,
                &key,
                &unexpectedValue);
        });

        auto result = co_await cppcoro::when_all_ready(
            move(operation1),
            move(operation2));

        EXPECT_NO_THROW(get<0>(result).result());
        EXPECT_THROW(get<1>(result).result(), WriteConflict);

        co_await ExpectGetTestRow(
            store,
            index,
            key.value(),
            expectedValue.value(),
            ToSequenceNumber(5),
            ToSequenceNumber(5));
    });
}

TEST_F(ProtoStoreTests, DISABLED_Can_commit_transaction)
{
    run_async([&]() -> task<>
    {
        auto store = co_await CreateMemoryStore();

        StringKey key;
        key.set_value("testKey1");
        StringValue expectedValue;
        expectedValue.set_value("testValue1");
        StringValue unexpectedValue;
        unexpectedValue.set_value("testValue2");
        TransactionId transactionId("transactionId1");

        auto index = co_await CreateTestIndex(
            store);

        co_await store->ExecuteOperation(
            BeginTransactionRequest(),
            [&](IOperation* operation)->task<>
        {
            co_await operation->AddRow(
                WriteOperationMetadata
                {
                    .TransactionId = &transactionId,
                },
                index,
                &key,
                &expectedValue);
        });

        {
            ReadRequest readRequest;
            readRequest.Key = &key;
            readRequest.Index = index;

            EXPECT_THROW(
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

TEST_F(ProtoStoreTests, PerformanceTest(Perf1))
{
    cppcoro::static_thread_pool threadPool(4);

    run_async([&]() -> task<>
    {
        auto store = co_await CreateMemoryStore();

        auto index = co_await CreateTestIndex(
            store);

        int valueCount = 100000;

        ranlux48 rng;

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
                        index,
                        &key,
                        &expectedValue);
                });
            }(value));
        }

        co_await asyncScope.join();

        std::vector<task<>> tasks;

        for (auto value : keys)
        {
            tasks.push_back([&](string myKey) -> task<>
            {
                co_await threadPool.schedule();

                StringKey key;
                key.set_value(myKey);
                StringValue expectedValue;
                expectedValue.set_value(myKey);

                ReadRequest readRequest;
                readRequest.Key = &key;
                readRequest.Index = index;

                auto readResult = co_await store->Read(
                    readRequest
                );

                StringValue actualValue;
                readResult.Value.unpack(&actualValue);

                auto messageDifferencerResult = MessageDifferencer::Equals(
                    expectedValue,
                    actualValue);

                EXPECT_TRUE(messageDifferencerResult);
            }(value));
        }

        co_await cppcoro::when_all(
            move(tasks));

        auto endTime = chrono::high_resolution_clock::now();

        auto runtimeMs = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);

        std::cout << "ProtoStoreTests runtime: " << runtimeMs.count() << "\r\n";
    });
}

std::atomic<long> Perf2_running_items(0);

TEST_F(ProtoStoreTests, PerformanceTest(Perf2))
{
    run_async([&]() -> task<>
    {
        CreateProtoStoreRequest createRequest;
        createRequest.ExtentStore = UseFilesystemStore("ProtoStoreTests_Perf2", "Perf2", 4096);
        createRequest.Schedulers = Schedulers::Default();

#ifdef NDEBUG
        createRequest.CheckpointLogSize = 10000000;
#else
        createRequest.CheckpointLogSize = 1000;
#endif

        auto store = co_await CreateStore(
            createRequest);

        auto index = co_await CreateTestIndex(
            store);

#ifdef NDEBUG
        int valueCount = 5000000;
#else
        int valueCount = 1000000;
#endif

        std::ranlux48 rng;
        uniform_int_distribution<int> distribution('a', 'z');

        auto keys = MakeRandomStrings(
            rng,
            20,
            valueCount
        );

        auto nonExistentKeys = MakeRandomStrings(
            rng,
            20,
            valueCount
        );

        auto startWriteTime = chrono::high_resolution_clock::now();

        cppcoro::async_scope asyncScopeWrite;
        auto schedulers = Schedulers::Default();

        auto keysPerInsert = 20;
        for (size_t startKeyIndex = 0; startKeyIndex < keys.size(); startKeyIndex += keysPerInsert)
        {
            auto endKeyIndex = std::min(
                startKeyIndex + keysPerInsert,
                keys.size());

            asyncScopeWrite.spawn([&](size_t startKeyIndex, size_t endKeyIndex) -> task<>
            {
                co_await schedulers.ComputeScheduler->schedule();
                Perf2_running_items.fetch_add(1);

                co_await store->ExecuteOperation(
                    BeginTransactionRequest(),
                    [&](IOperation* operation)->task<>
                {
                    for (auto myKeyIndex = startKeyIndex;
                        myKeyIndex < endKeyIndex;
                        myKeyIndex++)
                    {
                        auto& myKey = keys[myKeyIndex];
                        StringKey key;
                        key.set_value(myKey);
                        StringValue expectedValue;
                        expectedValue.set_value(myKey);

                        co_await operation->AddRow(
                            WriteOperationMetadata{},
                            index,
                            &key,
                            &expectedValue);
                    }
                });

                Perf2_running_items.fetch_sub(1);
            }(startKeyIndex, endKeyIndex));
        }

        co_await asyncScopeWrite.join();


        auto endWriteTime = chrono::high_resolution_clock::now();
        auto readWriteRuntimeMs = chrono::duration_cast<chrono::milliseconds>(endWriteTime - startWriteTime);

        std::cout << "ProtoStoreTests write runtime: " << readWriteRuntimeMs.count() << "\r\n" << std::flush;

        auto readLambda = [&]() -> task<>
        {
            auto startReadTime = chrono::high_resolution_clock::now();

            Perf2_running_items.store(0);

            std::vector<task<>> tasks;

#ifdef NDEBUG
            int threadCount = 50;
#else
            int threadCount = 1;
#endif
            for (int threadNumber = 0; threadNumber < threadCount; threadNumber++)
            {
                tasks.push_back([&](int threadNumber) -> task<>
                {
                    co_await schedulers.ComputeScheduler->schedule();

                    auto endKeyIndex = std::min(
                        keys.size() / threadCount * (threadNumber + 1) - 1,
                        keys.size());

                    for (auto keyIndex = keys.size() / threadCount * threadNumber;
                        keyIndex < endKeyIndex;
                        keyIndex++)
                    {
                        if (keyIndex % 1000 == 0)
                        {
                            co_await *schedulers.ComputeScheduler;
                        }

                        auto myKey = keys[keyIndex];

                        Perf2_running_items.fetch_add(1);

                        StringKey key;
                        key.set_value(myKey);
                        StringValue expectedValue;
                        expectedValue.set_value(myKey);

                        ReadRequest readRequest;
                        readRequest.Key = &key;
                        readRequest.Index = index;

                        auto readResult = co_await store->Read(
                            readRequest
                        );

                        StringValue actualValue;
                        readResult.Value.unpack(&actualValue);

                        auto messageDifferencerResult = MessageDifferencer::Equals(
                            expectedValue,
                            actualValue);

                        EXPECT_TRUE(messageDifferencerResult);

                        Perf2_running_items.fetch_sub(1);
                    }

                }(threadNumber));
            }

            co_await cppcoro::when_all(
                move(tasks));

            auto endReadTime = chrono::high_resolution_clock::now();
            auto readRuntimeMs = chrono::duration_cast<chrono::milliseconds>(endReadTime - startReadTime);

            std::cout << "ProtoStoreTests read runtime: " << readRuntimeMs.count() << "\r\n";
        };

        co_await readLambda();


        auto readNonExistentLambda = [&]() -> task<>
        {
            auto startReadTime = chrono::high_resolution_clock::now();

            Perf2_running_items.store(0);

            std::vector<task<>> tasks;

#ifdef NDEBUG
            int threadCount = 50;
#else
            int threadCount = 1;
#endif
            for (int threadNumber = 0; threadNumber < threadCount; threadNumber++)
            {
                tasks.push_back([&](int threadNumber) -> task<>
                {
                    auto endKeyIndex = std::min(
                        keys.size() / threadCount * (threadNumber + 1) - 1,
                        keys.size());

                    for (auto keyIndex = nonExistentKeys.size() / threadCount * threadNumber;
                        keyIndex < endKeyIndex;
                        keyIndex++)
                    {
                        if (keyIndex % 1000 == 0)
                        {
                            co_await *schedulers.ComputeScheduler;
                        }

                        auto myKey = nonExistentKeys[keyIndex];

                        Perf2_running_items.fetch_add(1);

                        StringKey key;
                        key.set_value(myKey);
                        StringValue expectedValue;
                        expectedValue.set_value(myKey);

                        ReadRequest readRequest;
                        readRequest.Key = &key;
                        readRequest.Index = index;

                        auto readResult = co_await store->Read(
                            readRequest
                        );

                        EXPECT_EQ(true, !readResult.Value);

                        Perf2_running_items.fetch_sub(1);
                    }

                }(threadNumber));
            }

            co_await cppcoro::when_all(
                move(tasks));

            auto endReadTime = chrono::high_resolution_clock::now();
            auto readRuntimeMs = chrono::duration_cast<chrono::milliseconds>(endReadTime - startReadTime);

            std::cout << "ProtoStoreTests read nonexistent runtime: " << readRuntimeMs.count() << "\r\n";
        };

        co_await readNonExistentLambda();

        auto checkpointLambda = [&]() -> task<>
        {
            auto checkpointStartTime = chrono::high_resolution_clock::now();
            co_await store->Checkpoint();
            auto checkpointEndTime = chrono::high_resolution_clock::now();
            auto checkpointRuntimeMs = chrono::duration_cast<chrono::milliseconds>(checkpointEndTime - checkpointStartTime);

            std::cout << "ProtoStoreTests checkpoint runtime: " << checkpointRuntimeMs.count() << "\r\n";
        };

        co_await checkpointLambda();

        co_await readLambda();
        co_await readNonExistentLambda();

        co_await checkpointLambda();
        co_await checkpointLambda();
        co_await checkpointLambda();

        co_await readLambda();
        co_await readNonExistentLambda();

        co_await checkpointLambda();

        co_await readLambda();
        co_await readNonExistentLambda();
    });
}

}
