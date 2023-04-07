#include "StandardIncludes.h"

#include "Phantom.ProtoStore/src/MemoryExtentStore.h"
#include "ProtoStoreTest.pb.h"
#include <cppcoro/single_consumer_event.hpp>
#include <cppcoro/async_scope.hpp>
#include <cppcoro/static_thread_pool.hpp>
#define NOMINMAX
#include "Windows.h"
#include "Phantom.ProtoStore/ProtoStoreTest_generated.h"
#include "TestFactories.h"

using namespace std;

namespace Phantom::ProtoStore
{

class ProtoStoreProtocolBufferTests
    :
    public testing::Test,
    public TestFactories
{
public:

    task<ProtoIndex> CreateTestProtoIndex(
        const shared_ptr<IProtoStore>& store
    )
    {
        CreateIndexRequest createIndexRequest;
        createIndexRequest.IndexName = "test_ProtoIndex";
        createIndexRequest.Schema = Schema::Make(
            StringKey::descriptor(),
            StringValue::descriptor());

        auto index = co_await store->CreateIndex(
            createIndexRequest
        );
        
        co_return *index;
    }

    task<ProtoIndex> GetTestProtoIndex(
        const shared_ptr<IProtoStore>& store
    )
    {
        GetIndexRequest getIndexRequest;
        getIndexRequest.IndexName = "test_ProtoIndex";

        auto index = co_await store->GetIndex(
            getIndexRequest
        );
        co_return index;
    }

    task<TransactionResult> AddRowToTestProtoIndex(
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

        co_return co_await store->ExecuteTransaction(
            BeginTransactionRequest(),
            [&](ITransaction* operation)->status_task<>
        {
            co_await co_await operation->AddRow(
                WriteOperationMetadata
                {
                    .ReadSequenceNumber = readSequenceNumber,
                    .WriteSequenceNumber = writeSequenceNumber,
                },
                index,
                &stringKey,
                value.has_value() ? & stringValue : nullptr);

            co_return{};
        });
    }

    task<> ExpectGetTestProtoRow(
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
            EXPECT_EQ(false, readResult->Value.has_value());
        }
        else
        {
            StringValue actualValue;
            readResult->Value.unpack(&actualValue);
            EXPECT_EQ(*expectedValue, actualValue.value());
            EXPECT_EQ(expectedSequenceNumber, readResult->WriteSequenceNumber);
        }
    }

    task<vector<row<StringKey, StringValue>>> EnumerateTestProtoRows(
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
            (*iterator)->Key.unpack(&resultRow.Key);
            (*iterator)->Value.unpack(&resultRow.Value);
            resultRow.ReadSequenceNumber = (*iterator)->WriteSequenceNumber;
            resultRow.WriteSequenceNumber = (*iterator)->WriteSequenceNumber;
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
        auto actualRows = co_await EnumerateTestProtoRows(
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

ASYNC_TEST_F(ProtoStoreProtocolBufferTests, CanCreate_memory_backed_store)
{
    auto store = CreateMemoryStore();
    co_return;
}
ASYNC_TEST_F(ProtoStoreProtocolBufferTests, CanOpen_memory_backed_store)
{
    auto storeFactory = MakeProtoStoreFactory();
    CreateProtoStoreRequest createRequest;

    createRequest.ExtentStore = UseMemoryExtentStore();

    auto store = co_await storeFactory->Create(
        createRequest);
    store.reset();

    store = co_await storeFactory->Open(
        createRequest);
}

ASYNC_TEST_F(ProtoStoreProtocolBufferTests, Open_fails_on_uncreated_store)
{
    auto storeFactory = MakeProtoStoreFactory();
    OpenProtoStoreRequest openRequest;

    openRequest.ExtentStore = UseMemoryExtentStore();

    EXPECT_THROW(
        co_await storeFactory->Open(
            openRequest),
        range_error);
}

ASYNC_TEST_F(ProtoStoreProtocolBufferTests, Can_read_and_write_one_row)
{
    auto store = co_await CreateMemoryStore();

    auto index = co_await CreateTestProtoIndex(
        store);

    co_await AddRowToTestProtoIndex(
        store,
        index,
        "testKey1",
        "testValue1",
        ToSequenceNumber(5));

    co_await ExpectGetTestProtoRow(
        store,
        index,
        "testKey1",
        "testValue1",
        ToSequenceNumber(5),
        ToSequenceNumber(5));
}

ASYNC_TEST_F(ProtoStoreProtocolBufferTests, Can_read_and_delete_and_enumerate_one_row)
{
    auto store = co_await CreateMemoryStore();

    auto index = co_await CreateTestProtoIndex(
        store);

    co_await AddRowToTestProtoIndex(
        store,
        index,
        "testKey1",
        "testValue1",
        ToSequenceNumber(5));

    co_await AddRowToTestProtoIndex(
        store,
        index,
        "testKey1",
        optional<string>(),
        ToSequenceNumber(6),
        ToSequenceNumber(5));

    auto enumerateAtSequenceNumber5 = co_await EnumerateTestProtoRows(
        store,
        index,
        optional<string>(),
        Inclusivity::Inclusive,
        optional<string>(),
        Inclusivity::Inclusive,
        ToSequenceNumber(5));

    auto enumerateAtSequenceNumber6 = co_await EnumerateTestProtoRows(
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
}
ASYNC_TEST_F(ProtoStoreProtocolBufferTests, Can_enumerate_one_row_after_add)
{
    auto store = co_await CreateMemoryStore();

    auto index = co_await CreateTestProtoIndex(
        store);

    co_await AddRowToTestProtoIndex(
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
}

ASYNC_TEST_F(ProtoStoreProtocolBufferTests, Can_enumerate_one_row_after_checkpoint)
{
    auto store = co_await CreateMemoryStore();

    auto index = co_await CreateTestProtoIndex(
        store);

    co_await AddRowToTestProtoIndex(
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
}

ASYNC_TEST_F(ProtoStoreProtocolBufferTests, Can_enumerate_one_row_after_update)
{
    auto store = co_await CreateMemoryStore();

    auto index = co_await CreateTestProtoIndex(
        store);

    co_await AddRowToTestProtoIndex(
        store,
        index,
        "testKey1",
        "testValue1",
        ToSequenceNumber(5));

    co_await AddRowToTestProtoIndex(
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
}
ASYNC_TEST_F(ProtoStoreProtocolBufferTests, Can_enumerate_one_row_after_checkpoint_and_update)
{
    auto store = co_await CreateMemoryStore();

    auto index = co_await CreateTestProtoIndex(
        store);

    co_await AddRowToTestProtoIndex(
        store,
        index,
        "testKey1",
        "testValue1",
        ToSequenceNumber(5));

    co_await store->Checkpoint();

    co_await AddRowToTestProtoIndex(
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
}
ASYNC_TEST_F(ProtoStoreProtocolBufferTests, Can_enumerate_one_row_after_two_checkpoint_and_update_and_checkpoint)
{
    auto store = co_await CreateMemoryStore();

    auto index = co_await CreateTestProtoIndex(
        store);

    co_await AddRowToTestProtoIndex(
        store,
        index,
        "testKey1",
        "testValue1",
        ToSequenceNumber(5));

    co_await store->Checkpoint();

    co_await AddRowToTestProtoIndex(
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
}
ASYNC_TEST_F(ProtoStoreProtocolBufferTests, Can_read_and_write_rows_after_checkpoints_and_merges)
{
    //auto createRequest = GetCreateFileStoreRequest("Can_read_and_write_rows_after_checkpoints_and_merges");
    auto createRequest = GetCreateMemoryStoreRequest();
    createRequest.DefaultMergeParameters.set_mergesperlevel(2);
    createRequest.DefaultMergeParameters.set_maxlevel(3);
    //auto createRequest = GetCreateMemoryStoreRequest();
    auto store = co_await CreateStore(createRequest);

    auto index = co_await CreateTestProtoIndex(
        store);

    ranlux48 rng;
    auto keys = MakeRandomStrings(
        rng,
        20,
        20);

    for (auto key : keys)
    {
        co_await AddRowToTestProtoIndex(
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
        co_await ExpectGetTestProtoRow(
            store,
            index,
            key,
            key + "-value",
            ToSequenceNumber(5),
            ToSequenceNumber(5));
    }

    store.reset();
    store = co_await OpenStore(createRequest);

    index = co_await GetTestProtoIndex(
        store);

    for (auto key : keys)
    {
        co_await ExpectGetTestProtoRow(
            store,
            index,
            key,
            key + "-value",
            ToSequenceNumber(5),
            ToSequenceNumber(5));
    }    
}

ASYNC_TEST_F(ProtoStoreProtocolBufferTests, Can_read_and_write_one_row_multiple_versions)
{
    auto store = co_await CreateMemoryStore();

    auto index = co_await CreateTestProtoIndex(
        store);

    co_await AddRowToTestProtoIndex(
        store,
        index,
        "testKey1",
        "testValue1-5",
        ToSequenceNumber(5));

    co_await AddRowToTestProtoIndex(
        store,
        index,
        "testKey1",
        "testValue1-6",
        ToSequenceNumber(6),
        ToSequenceNumber(5));

    co_await ExpectGetTestProtoRow(
        store,
        index,
        "testKey1",
        optional<string>(),
        ToSequenceNumber(4),
        ToSequenceNumber(5));

    co_await ExpectGetTestProtoRow(
        store,
        index,
        "testKey1",
        "testValue1-5",
        ToSequenceNumber(5),
        ToSequenceNumber(5));

    co_await ExpectGetTestProtoRow(
        store,
        index,
        "testKey1",
        "testValue1-6",
        ToSequenceNumber(6),
        ToSequenceNumber(6));

    co_await ExpectGetTestProtoRow(
        store,
        index,
        "testKey1",
        "testValue1-6",
        ToSequenceNumber(7),
        ToSequenceNumber(6));
}
ASYNC_TEST_F(ProtoStoreProtocolBufferTests, Can_read_and_write_one_row_multiple_versions_after_checkpoints)
{
    auto store = co_await CreateMemoryStore();

    auto index = co_await CreateTestProtoIndex(
        store);

    co_await AddRowToTestProtoIndex(
        store,
        index,
        "testKey1",
        "testValue1-5",
        ToSequenceNumber(5));

    co_await store->Checkpoint();

    co_await AddRowToTestProtoIndex(
        store,
        index,
        "testKey1",
        "testValue1-6",
        ToSequenceNumber(6),
        ToSequenceNumber(5));

    co_await store->Checkpoint();

    co_await AddRowToTestProtoIndex(
        store,
        index,
        "testKey1",
        "testValue1-7",
        ToSequenceNumber(7),
        ToSequenceNumber(6));

    co_await ExpectGetTestProtoRow(
        store,
        index,
        "testKey1",
        optional<string>(),
        ToSequenceNumber(4),
        ToSequenceNumber(5));

    co_await ExpectGetTestProtoRow(
        store,
        index,
        "testKey1",
        "testValue1-5",
        ToSequenceNumber(5),
        ToSequenceNumber(5));

    co_await ExpectGetTestProtoRow(
        store,
        index,
        "testKey1",
        "testValue1-6",
        ToSequenceNumber(6),
        ToSequenceNumber(6));

    co_await ExpectGetTestProtoRow(
        store,
        index,
        "testKey1",
        "testValue1-7",
        ToSequenceNumber(7),
        ToSequenceNumber(7));

    co_await ExpectGetTestProtoRow(
        store,
        index,
        "testKey1",
        "testValue1-7",
        ToSequenceNumber(8),
        ToSequenceNumber(7));
}
ASYNC_TEST_F(ProtoStoreProtocolBufferTests, Can_conflict_after_row_written)
{
    auto store = co_await CreateMemoryStore();

    auto index = co_await CreateTestProtoIndex(
        store);

    co_await AddRowToTestProtoIndex(
        store,
        index,
        "testKey1",
        "testValue1",
        ToSequenceNumber(5));

    auto result =
        co_await AddRowToTestProtoIndex(
            store,
            index,
            "testKey1",
            "testValue2",
            ToSequenceNumber(4));
    EXPECT_EQ((std::unexpected
        {
            FailedResult
            {
                .ErrorCode = make_error_code(ProtoStoreErrorCode::AbortedTransaction),
                .ErrorDetails = TransactionFailedResult
                {
                    .TransactionOutcome = TransactionOutcome::Aborted,
                    //.Failures =
                    //{
                    //    {
                    //        .ErrorCode = make_error_code(ProtoStoreErrorCode::WriteConflict),
                    //        .ErrorDetails = WriteConflict
                    //        {
                    //            .Index = index,
                    //            .ConflictingSequenceNumber = ToSequenceNumber(5),
                    //        }
                    //    }
                    //}
                }
            }
        }),
        result);

    co_await ExpectGetTestProtoRow(
        store,
        index,
        "testKey1",
        "testValue1",
        ToSequenceNumber(5),
        ToSequenceNumber(5));

    co_await ExpectGetTestProtoRow(
        store,
        index,
        "testKey1",
        optional<string>(),
        ToSequenceNumber(4),
        ToSequenceNumber(5));
}
ASYNC_TEST_F(ProtoStoreProtocolBufferTests, Can_conflict_after_row_checkpointed)
{
    auto store = co_await CreateMemoryStore();

    auto index = co_await CreateTestProtoIndex(
        store);

    co_await AddRowToTestProtoIndex(
        store,
        index,
        "testKey1",
        "testValue1",
        ToSequenceNumber(5));

    co_await store->Checkpoint();

    auto result =
        co_await AddRowToTestProtoIndex(
            store,
            index,
            "testKey1",
            "testValue2",
            ToSequenceNumber(4));
    EXPECT_EQ((std::unexpected
        {
            FailedResult
            {
                .ErrorCode = make_error_code(ProtoStoreErrorCode::AbortedTransaction),
                .ErrorDetails = TransactionFailedResult
                {
                    .TransactionOutcome = TransactionOutcome::Aborted,
                    //.Failures = 
                    //{
                    //    {
                    //        .ErrorCode = make_error_code(ProtoStoreErrorCode::WriteConflict),
                    //        .ErrorDetails = WriteConflict
                    //        {
                    //            .Index = index,
                    //            .ConflictingSequenceNumber = ToSequenceNumber(5),
                    //        }
                    //    }
                    //}
                }
            }
        }),
        result);

    co_await ExpectGetTestProtoRow(
        store,
        index,
        "testKey1",
        "testValue1",
        ToSequenceNumber(5),
        ToSequenceNumber(5));

    co_await ExpectGetTestProtoRow(
        store,
        index,
        "testKey1",
        optional<string>(),
        ToSequenceNumber(4),
        ToSequenceNumber(5));
}
ASYNC_TEST_F(ProtoStoreProtocolBufferTests, Can_read_and_write_one_row_after_reopen)
{
    //auto createRequest = GetCreateMemoryStoreRequest();
    auto createRequest = GetCreateFileStoreRequest("Can_read_and_write_one_row_after_reopen");

    auto store = co_await CreateStore(createRequest);

    auto index = co_await CreateTestProtoIndex(
        store);

    co_await AddRowToTestProtoIndex(
        store,
        index,
        "testKey1",
        "testValue1",
        ToSequenceNumber(5));

    store.reset();
    store = co_await OpenStore(createRequest);

    index = co_await GetTestProtoIndex(
        store);

    co_await ExpectGetTestProtoRow(
        store,
        index,
        "testKey1",
        "testValue1",
        ToSequenceNumber(5),
        ToSequenceNumber(5));
}
ASYNC_TEST_F(ProtoStoreProtocolBufferTests, Checkpoint_deletes_old_logs)
{
    //auto createRequest = GetCreateFileStoreRequest("Checkpoint_deletes_old_logs");
    auto createRequest = GetCreateMemoryStoreRequest();

    auto store = co_await CreateStore(createRequest);

    auto memoryStore = std::static_pointer_cast<MemoryExtentStore>(
        co_await createRequest.ExtentStore());

    auto index = co_await CreateTestProtoIndex(
        store);

    co_await AddRowToTestProtoIndex(
        store,
        index,
        "testKey1",
        "testValue1",
        ToSequenceNumber(5));

    // Checkpoint twice to ensure old log is delete.
    co_await store->Checkpoint();
    EXPECT_EQ(true, co_await memoryStore->ExtentExists(FlatValue(MakeLogExtentName(0))));

    co_await store->Checkpoint();

    EXPECT_EQ(false, co_await memoryStore->ExtentExists(FlatValue(MakeLogExtentName(0))));
}

ASYNC_TEST_F(ProtoStoreProtocolBufferTests, Can_read_and_write_one_row_after_checkpoint)
{
    auto createRequest = GetCreateMemoryStoreRequest();

    auto store = co_await CreateStore(createRequest);

    auto index = co_await CreateTestProtoIndex(
        store);

    co_await AddRowToTestProtoIndex(
        store,
        index,
        "testKey1",
        "testValue1",
        ToSequenceNumber(5));

    co_await store->Checkpoint();

    co_await ExpectGetTestProtoRow(
        store,
        index,
        "testKey1",
        "testValue1",
        ToSequenceNumber(5),
        ToSequenceNumber(5));
}
ASYNC_TEST_F(ProtoStoreProtocolBufferTests, Can_read_and_write_one_row_after_checkpoint_and_reopen)
{
    auto createRequest = GetCreateFileStoreRequest("Can_read_and_write_one_row_after_checkpoint_and_reopen");

    auto store = co_await CreateStore(createRequest);

    auto index = co_await CreateTestProtoIndex(
        store);

    co_await AddRowToTestProtoIndex(
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

    index = co_await GetTestProtoIndex(
        store);

    co_await ExpectGetTestProtoRow(
        store,
        index,
        "testKey1",
        "testValue1",
        ToSequenceNumber(5),
        ToSequenceNumber(5));
}
ASYNC_TEST_F(ProtoStoreProtocolBufferTests, DISABLED_Can_read_written_row_during_operation)
{
    auto store = co_await CreateMemoryStore();

    StringKey key;
    key.set_value("testKey1");
    StringValue expectedValue;
    expectedValue.set_value("testValue1");

    auto index = co_await CreateTestProtoIndex(
        store);

    co_await store->ExecuteTransaction(
        BeginTransactionRequest(),
        [&](ITransaction* operation)->status_task<>
    {
        co_await co_await operation->AddRow(
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
        readResult->Value.unpack(&actualValue);
    }

    co_return{};
    });
}

ASYNC_TEST_F(ProtoStoreProtocolBufferTests, Can_conflict_on_one_row_and_commits_first)
{
    auto store = co_await CreateMemoryStore();

    StringKey key;
    key.set_value("testKey1");
    StringValue expectedValue;
    expectedValue.set_value("testValue1");
    StringValue unexpectedValue;
    unexpectedValue.set_value("testValue2");

    auto index = co_await CreateTestProtoIndex(
        store);

    cppcoro::single_consumer_event addRowEvent;

    auto operation1 = store->ExecuteTransaction(
        BeginTransactionRequest(),
        [&](ITransaction* operation)->status_task<>
    {
        co_await co_await operation->AddRow(
            WriteOperationMetadata
            {
                .WriteSequenceNumber = ToSequenceNumber(5),
            },
            index,
            &key,
            &expectedValue);

    addRowEvent.set();

    co_return{};
    });

    auto operation2 = store->ExecuteTransaction(
        BeginTransactionRequest(),
        [&](ITransaction* operation)->status_task<>
    {
        co_await addRowEvent;
    co_await co_await operation->AddRow(
        WriteOperationMetadata
        {
            .WriteSequenceNumber = ToSequenceNumber(4),
        },
        index,
        &key,
        &unexpectedValue);

    co_return{};
    });

    co_await operation1.when_ready();
    co_await operation2.when_ready();

    auto result = co_await operation1;
    EXPECT_EQ(result, TransactionSucceededResult{ TransactionOutcome::Committed });
    result = co_await operation2;
    EXPECT_EQ((std::unexpected
        {
            FailedResult
            {
                .ErrorCode = make_error_code(ProtoStoreErrorCode::AbortedTransaction),
                .ErrorDetails = TransactionFailedResult
                {
                    .TransactionOutcome = TransactionOutcome::Aborted,
                    //.Failures =
                    //{
                    //    {
                    //        .ErrorCode = make_error_code(ProtoStoreErrorCode::WriteConflict),
                    //        .ErrorDetails = WriteConflict
                    //        {
                    //            .Index = index,
                    //            .ConflictingSequenceNumber = ToSequenceNumber(5),
                    //        }
                    //    }
                    //}
                }
            }
        }),
        result);

    co_await ExpectGetTestProtoRow(
        store,
        index,
        key.value(),
        expectedValue.value(),
        ToSequenceNumber(5),
        ToSequenceNumber(5));
}
ASYNC_TEST_F(ProtoStoreProtocolBufferTests, DISABLED_Can_commit_transaction_in_memory_table_from_another_transaction)
{
    auto store = co_await CreateMemoryStore();

    StringKey key;
    key.set_value("testKey1");
    StringValue expectedValue;
    expectedValue.set_value("testValue1");
    StringValue unexpectedValue;
    unexpectedValue.set_value("testValue2");
    TransactionId transactionId("transactionId1");

    auto index = co_await CreateTestProtoIndex(
        store);

    co_await store->ExecuteTransaction(
        BeginTransactionRequest(),
        [&](ITransaction* operation)->status_task<>
    {
        co_await co_await operation->AddRow(
            WriteOperationMetadata
            {
                .TransactionId = transactionId,
            },
            index,
            &key,
            &expectedValue);

    co_return{};
    });

    {
        ReadRequest readRequest;
        readRequest.Key = &key;
        readRequest.Index = index;

        auto result = co_await store->Read(
            readRequest);
        EXPECT_EQ(
            (std::unexpected
                {
                    FailedResult
                    {
                        .ErrorCode = make_error_code(ProtoStoreErrorCode::UnresolvedTransaction),
                        .ErrorDetails = UnresolvedTransaction
                        {
                            .UnresolvedTransactionId = transactionId,
                        },
                    }
                }),
            result);
    }

    co_await store->ExecuteTransaction(
        BeginTransactionRequest(),
        [&](ITransaction* operation)->status_task<>
    {
        co_await operation->ResolveTransaction(
            WriteOperationMetadata
            {
                .TransactionId = transactionId,
            },
            TransactionOutcome::Committed);

    co_return{};
    });

    {
        ReadRequest readRequest;
        readRequest.Key = &key;
        readRequest.Index = index;

        auto readResult = co_await store->Read(
            readRequest
        );

        StringValue actualValue;
        readResult->Value.unpack(&actualValue);
    }
}
ASYNC_TEST_F(ProtoStoreProtocolBufferTests, DISABLED_Can_commit_transaction_in_memory_table_from_replayed_store)
{
    auto createRequest = GetCreateMemoryStoreRequest();
    auto store = co_await CreateStore(createRequest);

    StringKey key;
    key.set_value("testKey1");
    StringValue expectedValue;
    expectedValue.set_value("testValue1");
    StringValue unexpectedValue;
    unexpectedValue.set_value("testValue2");
    TransactionId transactionId("transactionId1");

    auto index = co_await CreateTestProtoIndex(
        store);

    co_await store->ExecuteTransaction(
        BeginTransactionRequest(),
        [&](ITransaction* operation)->status_task<>
    {
        co_await co_await operation->AddRow(
            WriteOperationMetadata
            {
                .TransactionId = transactionId,
                .WriteSequenceNumber = ToSequenceNumber(5),
            },
            index,
            &key,
            &expectedValue);

    co_return{};
    });

    // Close and reopen the store, thus replaying the transactions.
    // The transaction should still be unresolved.
    store.reset();
    store = co_await OpenStore(createRequest);

    {
        ReadRequest readRequest;
        readRequest.Key = &key;
        readRequest.Index = index;

        auto result = co_await store->Read(
            readRequest);
        EXPECT_EQ(
            (std::unexpected
                {
                    FailedResult
                    {
                        .ErrorCode = make_error_code(ProtoStoreErrorCode::UnresolvedTransaction),
                        .ErrorDetails = UnresolvedTransaction
                        {
                            .UnresolvedTransactionId = transactionId,
                        },
                    }
                }),
            result);
    }

    // Now resolve the transaction.
    co_await store->ExecuteTransaction(
        BeginTransactionRequest(),
        [&](ITransaction* operation)->status_task<>
    {
        co_await operation->ResolveTransaction(
            WriteOperationMetadata
            {
                .TransactionId = transactionId,
            },
            TransactionOutcome::Committed);

    co_return{};
    });

    // The transaction should work!
    {
        co_await ExpectGetTestProtoRow(
            store,
            index,
            key.value(),
            expectedValue.value(),
            SequenceNumber::Latest,
            ToSequenceNumber(5)
        );
    }

    // Close and reopen the store, thus replaying the transactions.
    store.reset();
    store = co_await OpenStore(createRequest);

    {
        co_await ExpectGetTestProtoRow(
            store,
            index,
            key.value(),
            expectedValue.value(),
            SequenceNumber::Latest,
            ToSequenceNumber(1)
        );
    }
}

cppcoro::async_scope* m_perfScope;
shared_ptr<IProtoStore>* m_perfStore;

ASYNC_TEST_F(ProtoStoreProtocolBufferTests, PerformanceTest(Perf1))
{
    cppcoro::static_thread_pool threadPool;

    auto store = co_await CreateMemoryStore();
    m_perfStore = &store;

    auto index = co_await CreateTestProtoIndex(
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

        co_await store->ExecuteTransaction(
            BeginTransactionRequest(),
            [&](ITransaction* operation)->status_task<>
        {
            co_await co_await operation->AddRow(
                WriteOperationMetadata{},
                index,
                &key,
                &expectedValue);

        co_return{};
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
        readResult->Value.unpack(&actualValue);

        auto messageDifferencerResult = MessageDifferencer::Equals(
            expectedValue,
            actualValue);

        EXPECT_TRUE(messageDifferencerResult);
        }(value));
    }

    for (auto& task : tasks)
    {
        co_await task;
    }

    auto endTime = chrono::high_resolution_clock::now();

    auto runtimeMs = chrono::duration_cast<chrono::milliseconds>(endTime - startTime);

    std::cout << "ProtoStoreTests runtime: " << runtimeMs.count() << "\r\n";
}

std::atomic<long> Perf2_running_items(0);
std::vector<shared_task<>> performanceTasks;

ASYNC_TEST_F(ProtoStoreProtocolBufferTests, PerformanceTest(Perf2))
{
    CreateProtoStoreRequest createRequest;
    createRequest.ExtentStore = UseFilesystemStore("ProtoStoreProtocolBufferTests_Perf2", "Perf2", 4096);
    createRequest.Schedulers = Schedulers::Default();

#ifdef NDEBUG
    createRequest.CheckpointLogSize = 1000000;
#else
    createRequest.CheckpointLogSize = 100000;
#endif

    auto store = co_await CreateStore(
        createRequest);
    m_perfStore = &store;

    auto index = co_await CreateTestProtoIndex(
        store);

#ifdef NDEBUG
    int valueCount = 5000000;
#else
    int valueCount = 10000;
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

    std::atomic<size_t> rowsWritten = 0;

    auto writeItemLambda = [&](size_t startKeyIndex, size_t endKeyIndex) -> task<>
    {
        co_await schedulers.ComputeScheduler->schedule();
        Perf2_running_items.fetch_add(1);

        bool doCheckpoint = false;

        co_await store->ExecuteTransaction(
            BeginTransactionRequest(),
            [&](ITransaction* operation)->status_task<>
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

                co_await co_await operation->AddRow(
                    WriteOperationMetadata{},
                    index,
                    &key,
                    &expectedValue);

                if (rowsWritten.fetch_add(1) % 10000 == 0)
                {
                    doCheckpoint = true;
                }
            }
            co_return{};
        });

        if (doCheckpoint)
        {
            //co_await store->Checkpoint();
            //co_await store->Merge();
        }

        Perf2_running_items.fetch_sub(1);
    };

    auto keysPerInsert = 100;
    for (size_t startKeyIndex = 0; startKeyIndex < keys.size(); startKeyIndex += keysPerInsert)
    {
        auto endKeyIndex = std::min(
            startKeyIndex + keysPerInsert,
            keys.size());

        asyncScopeWrite.spawn(writeItemLambda(startKeyIndex, endKeyIndex));
    }

    co_await asyncScopeWrite.join();

    auto endWriteTime = chrono::high_resolution_clock::now();
    auto writeRuntimeMs = chrono::duration_cast<chrono::milliseconds>(endWriteTime - startWriteTime);

    std::cout << "ProtoStoreTests write runtime: " << writeRuntimeMs.count() << "ms\r\n" << std::flush;

    auto readLambda = [&]() -> shared_task<>
    {
        auto startReadTime = chrono::high_resolution_clock::now();

        Perf2_running_items.store(0);

#ifdef NDEBUG
        int threadCount = valueCount;
#else
        int threadCount = 1;
#endif
        auto readItemLambda = [&](int threadNumber) -> shared_task<>
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
                readResult->Value.unpack(&actualValue);

                auto messageDifferencerResult = MessageDifferencer::Equals(
                    expectedValue,
                    actualValue);

                EXPECT_TRUE(messageDifferencerResult);

                Perf2_running_items.fetch_sub(1);
            }
        };

        cppcoro::async_scope asyncScopeRead;
        m_perfScope = &asyncScopeRead;

        for (int threadNumber = 0; threadNumber < threadCount; threadNumber++)
        {
            performanceTasks.push_back(
                readItemLambda(threadNumber));
            asyncScopeRead.spawn(
                performanceTasks.back());
        }

        co_await asyncScopeRead.join();

        auto endReadTime = chrono::high_resolution_clock::now();
        auto readRuntimeMs = chrono::duration_cast<chrono::milliseconds>(endReadTime - startReadTime);

        std::cout << "ProtoStoreTests read runtime: " << readRuntimeMs.count() << "ms\r\n";
    };

    co_await readLambda();


    auto readNonExistentLambda = [&]() -> task<>
    {
        auto startReadTime = chrono::high_resolution_clock::now();

        Perf2_running_items.store(0);

#ifdef NDEBUG
        int threadCount = 50;
#else
        int threadCount = 1;
#endif
        auto readItemsLambda = [&](int threadNumber) -> shared_task<>
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

                EXPECT_EQ(true, !readResult->Value);

                Perf2_running_items.fetch_sub(1);
            }
        };

        cppcoro::async_scope asyncScopeRead;
        m_perfScope = &asyncScopeRead;

        for (int threadNumber = 0; threadNumber < threadCount; threadNumber++)
        {
            performanceTasks.push_back(readItemsLambda(threadNumber));
            asyncScopeRead.spawn(performanceTasks.back());
        }

        co_await asyncScopeRead.join();

        auto endReadTime = chrono::high_resolution_clock::now();
        auto readRuntimeMs = chrono::duration_cast<chrono::milliseconds>(endReadTime - startReadTime);

        std::cout << "ProtoStoreTests read nonexistent runtime: " << readRuntimeMs.count() << "ms\r\n";
    };

    co_await readNonExistentLambda();

    auto checkpointLambda = [&]() -> task<>
    {
        auto checkpointStartTime = chrono::high_resolution_clock::now();
        co_await store->Checkpoint();
        auto checkpointEndTime = chrono::high_resolution_clock::now();
        auto checkpointRuntimeMs = chrono::duration_cast<chrono::milliseconds>(checkpointEndTime - checkpointStartTime);

        std::cout << "ProtoStoreTests checkpoint runtime: " << checkpointRuntimeMs.count() << "ms\r\n";
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
}

}
