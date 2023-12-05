#include "StandardIncludes.h"
#include "Phantom.Coroutines/async_scope.h"
#include "Phantom.ProtoStore/src/MemoryTableImpl.h"
#include "Phantom.ProtoStore/src/ProtocolBuffersValueComparer.h"
#include "ProtoStoreTest.pb.h"
#include <optional>
#include <string>
#include <tuple>
#include <vector>

using namespace std;

namespace Phantom::ProtoStore
{
class MemoryTableTests : public ::testing::Test
{
    static_assert(static_cast<uint64_t>(MemoryTableOutcomeAndSequenceNumber::OutcomeAborted) == static_cast<uint64_t>(TransactionOutcome::Aborted));
    static_assert(static_cast<uint64_t>(MemoryTableOutcomeAndSequenceNumber::OutcomeCommitted) == static_cast<uint64_t>(TransactionOutcome::Committed));
    static_assert(static_cast<uint64_t>(MemoryTableOutcomeAndSequenceNumber::OutcomeUnknown) == static_cast<uint64_t>(TransactionOutcome::Unknown));

protected:
    std::shared_ptr<Schema> schema;
    std::shared_ptr<ValueComparer> keyComparer;
    std::shared_ptr<MemoryTable> memoryTable;

public:
    MemoryTableTests()
        : 
        schema(
            std::make_shared<Schema>(
                KeySchema{ StringKey::descriptor() },
                ValueSchema{ StringValue::descriptor() }
            )),
        keyComparer(
            std::make_shared<ProtocolBuffersValueComparer>(
                StringKey::descriptor())),
        memoryTable(
            std::make_shared<MemoryTable>(
                schema,
                keyComparer))
    {}

protected:
    shared_ptr<DelayedMemoryTableTransactionOutcome> WithOutcome(
        MemoryTableTransactionSequenceNumber transactionSequenceNumber,
        TransactionOutcome outcome,
        uint64_t writeSequenceNumber)
    {
        auto delayedOutcome = make_shared<DelayedMemoryTableTransactionOutcome>(
            transactionSequenceNumber);
        if (outcome != TransactionOutcome::Unknown)
        {
            if (outcome == TransactionOutcome::Committed)
            {
                delayedOutcome->BeginCommit(
                    ToSequenceNumber(writeSequenceNumber));
            }
            delayedOutcome->Complete();
        }

        return delayedOutcome;
    }

    std::unique_ptr<FlatBuffers::DataValueT> ToDataValue(
        std::span<const std::byte> bytes)
    {
        auto result = std::make_unique<FlatBuffers::DataValueT>();
        result->flatbuffers_alignment = 1;
        result->data.assign_range(get_int8_t_span(bytes));
        return std::move(result);
    }

    task<FlatMessage<FlatBuffers::LoggedRowWrite>> MakeLoggedRowWrite(
        const Message& rowKey,
        const Message& rowValue,
        uint64_t writeSequenceNumber,
        std::optional<TransactionId> transactionId = {}
        )
    {
        auto rowKeyProto = ProtoValue(&rowKey).pack();
        auto rowValueProto = ProtoValue(&rowValue).pack();

        FlatBuffers::LoggedRowWriteT loggedRowWrite;
        if (transactionId)
        {
            loggedRowWrite.distributed_transaction_id = *transactionId;
        }
        loggedRowWrite.key = ToDataValue(
            rowKeyProto.as_protocol_buffer_bytes_if());

        loggedRowWrite.value = ToDataValue(
            rowValueProto.as_protocol_buffer_bytes_if());
        loggedRowWrite.sequence_number = writeSequenceNumber;

        co_return FlatMessage{ loggedRowWrite };
    }

    task<FlatMessage<FlatBuffers::LoggedRowWrite>> MakeLoggedRowWrite(
        string key,
        string value,
        uint64_t writeSequenceNumber,
        std::optional<TransactionId> transactionId = {}
    )
    {
        StringKey rowKey;
        rowKey.set_value(key);
        StringValue rowValue;
        rowValue.set_value(value);

        co_return co_await MakeLoggedRowWrite(
            rowKey,
            rowValue,
            writeSequenceNumber,
            transactionId
        );
    }

    task<shared_ptr<DelayedMemoryTableTransactionOutcome>> AddRow(
        MemoryTableTransactionSequenceNumber transactionSequenceNumber,
        const Message& rowKey,
        const Message& rowValue,
        uint64_t writeSequenceNumber,
        uint64_t readSequenceNumber,
        TransactionOutcome outcome = TransactionOutcome::Committed,
        std::optional<TransactionId> transactionId = {},
        std::optional<SequenceNumber>* conflictingSequenceNumber = nullptr)
    {
        auto loggedRowWriteMessage = co_await MakeLoggedRowWrite(
            rowKey,
            rowValue,
            writeSequenceNumber,
            transactionId);

        auto delayedOutcome = WithOutcome(transactionSequenceNumber, outcome, writeSequenceNumber);

        auto result = co_await memoryTable->AddRow(
            ToSequenceNumber(readSequenceNumber),
            std::move(loggedRowWriteMessage),
            delayedOutcome);

        if (conflictingSequenceNumber)
        {
            *conflictingSequenceNumber = result;
        }

        co_return delayedOutcome;
    }

    task<shared_ptr<DelayedMemoryTableTransactionOutcome>> AddRow(
        MemoryTableTransactionSequenceNumber transactionSequenceNumber,
        string key,
        string value,
        uint64_t writeSequenceNumber,
        uint64_t readSequenceNumber,
        TransactionOutcome outcome = TransactionOutcome::Committed,
        std::optional<TransactionId> transactionId = {},
        std::optional<SequenceNumber>* conflictingSequenceNumber = nullptr)
    {
        StringKey rowKey;
        rowKey.set_value(key);
        StringValue rowValue;
        rowValue.set_value(value);

        co_return co_await AddRow(
            transactionSequenceNumber,
            rowKey,
            rowValue,
            writeSequenceNumber,
            readSequenceNumber,
            outcome,
            transactionId,
            conflictingSequenceNumber);
    }

    struct ExpectedRow
    {
        string Key;
        string Value;
        uint64_t SequenceNumber;
        std::optional<std::string> TransactionId;

        auto operator <=>(const ExpectedRow&) const = default;
    };

    task<> EnumerateExpectedRows(
        shared_ptr<DelayedMemoryTableTransactionOutcome> delayedTransactionOutcome,
        uint64_t readSequenceNumber,
        vector<ExpectedRow> expectedRows,
        optional<string> keyLow = optional<string>(),
        optional<string> keyHigh = optional<string>(),
        Inclusivity keyLowInclusivity = Inclusivity::Inclusive,
        Inclusivity keyHighInclusivity = Inclusivity::Exclusive
    )
    {
        ProtoValue keyLowProto;
        ProtoValue keyHighProto;

        StringKey keyLowMessage;
        if (keyLow.has_value())
        {
            keyLowMessage.set_value(
                keyLow.value());
            keyLowProto = ProtoValue(&keyLowMessage).pack();
        }

        StringKey keyHighMessage;
        if (keyHigh.has_value())
        {
            keyHighMessage.set_value(
                keyHigh.value());
            keyHighProto = ProtoValue(&keyHighMessage).pack();
        }

        auto enumeration = memoryTable->Enumerate(
            delayedTransactionOutcome,
            ToSequenceNumber(readSequenceNumber),
            {
                .Key = keyLowProto,
                .Inclusivity = keyLowInclusivity,
            },
            {
                .Key = keyHighProto,
                .Inclusivity = keyHighInclusivity
            });

        vector<ExpectedRow> storedRows;

        for (auto iterator = co_await enumeration.begin();
            iterator != enumeration.end();
            co_await ++iterator)
        {
            ResultRow& row = *iterator;

            StringKey resultKey;
            row.Key.unpack(&resultKey);
            StringValue resultValue;
            row.Value.unpack(&resultValue);
            
            storedRows.push_back(
                ExpectedRow
                {
                    .Key = resultKey.value(),
                    .Value = resultValue.value(),
                    .SequenceNumber = ToUint64(row.WriteSequenceNumber),
                    .TransactionId = row.TransactionId
                        ? row.TransactionId->c_str()
                        : std::optional<std::string>(),
                }
            );
        }

        EXPECT_EQ(
            expectedRows,
            storedRows);

        co_await memoryTable->Join();
    }

    task<std::optional<SequenceNumber>> CheckForWriteConflict(
        const shared_ptr<DelayedMemoryTableTransactionOutcome>& delayedTransactionOutcome,
        const std::string& key,
        SequenceNumber readSequenceNumber
    )
    {
        StringKey rowKey;
        rowKey.set_value(key);

        auto rowKeyProto = ProtoValue(&rowKey).pack();

        auto result = co_await memoryTable->CheckForWriteConflict(
            delayedTransactionOutcome,
            readSequenceNumber,
            rowKeyProto);

        co_return result;
    }
};

ASYNC_TEST_F(MemoryTableTests, GetLatestSequenceNumber_starts_at_zero)
{
    EXPECT_EQ(ToSequenceNumber(0), memoryTable->GetLatestSequenceNumber());
    co_return;
}

ASYNC_TEST_F(MemoryTableTests, ReplayRow_updates_GetLatestSequenceNumber_to_max_of_current_value_and_new_row)
{
    EXPECT_EQ(ToSequenceNumber(0), memoryTable->GetLatestSequenceNumber());

    auto loggedRow = co_await MakeLoggedRowWrite(
        "key1",
        "value1",
        5);

    co_await memoryTable->ReplayRow(
        loggedRow);

    EXPECT_EQ(ToSequenceNumber(5), memoryTable->GetLatestSequenceNumber());

    loggedRow = co_await MakeLoggedRowWrite(
        "key2",
        "value2",
        3);

    co_await memoryTable->ReplayRow(
        loggedRow);

    EXPECT_EQ(ToSequenceNumber(5), memoryTable->GetLatestSequenceNumber());
    
    loggedRow = co_await MakeLoggedRowWrite(
        "key3",
        "value3",
        10);

    co_await memoryTable->ReplayRow(
        loggedRow);

    EXPECT_EQ(ToSequenceNumber(10), memoryTable->GetLatestSequenceNumber());
    
    co_await memoryTable->Join();
}

ASYNC_TEST_F(MemoryTableTests, Can_add_and_enumerate_one_row)
{
    co_await AddRow(
        0,
        "key-1",
        "value-1",
        5,
        0,
        TransactionOutcome::Committed,
        "transaction id"
    );

    co_await EnumerateExpectedRows(
        0,
        5,
        {
            {"key-1", "value-1", 5, "transaction id"},
        }
    );

    co_await memoryTable->Join();
}

ASYNC_TEST_F(MemoryTableTests, Can_add_distinct_rows)
{
    co_await AddRow(
        0,
        "key-1",
        "value-1",
        5,
        0
    );

    co_await AddRow(
        0,
        "key-2",
        "value-2",
        5,
        0
    );

    co_await EnumerateExpectedRows(
        0,
        5,
        {
            {"key-1", "value-1", 5},
            {"key-2", "value-2", 5},
        }
    );

    co_await memoryTable->Join();
}

ASYNC_TEST_F(MemoryTableTests, Enumerate_skips_aborted_rows)
{
    co_await AddRow(
        0,
        "key-1",
        "value-1",
        5,
        0
    );

    co_await AddRow(
        1,
        "key-2",
        "value-2",
        5,
        0,
        TransactionOutcome::Aborted
    );

    co_await EnumerateExpectedRows(
        nullptr,
        5,
        {
            {"key-1", "value-1", 5},
        }
    );

    co_await memoryTable->Join();
}

ASYNC_TEST_F(MemoryTableTests, Fail_to_add_write_conflict_from_ReadSequenceNumber)
{
    co_await AddRow(
        0,
        "key-1",
        "value-1",
        5,
        0
    );

    std::optional<SequenceNumber> conflictingSequenceNumber;

    co_await AddRow(
        0,
        "key-1",
        "value-1-2",
        6,
        0,
        TransactionOutcome::Committed,
        {},
        &conflictingSequenceNumber
    );

    EXPECT_EQ(ToSequenceNumber(5), conflictingSequenceNumber);

    co_await EnumerateExpectedRows(
        0,
        5,
        {
            {"key-1", "value-1", 5},
        }
    );

    co_await EnumerateExpectedRows(
        0,
        6,
        {
            {"key-1", "value-1", 5},
        }
    );

    co_await memoryTable->Join();
}

ASYNC_TEST_F(MemoryTableTests, Fail_to_add_write_conflict_from_Committed_Row)
{
    co_await AddRow(
        0,
        "key-1",
        "value-1",
        5,
        0
    );

    std::optional<SequenceNumber> conflictingSequenceNumber;

    co_await AddRow(
        0,
        "key-1",
        "value-1-2",
        5,
        7,
        TransactionOutcome::Unknown,
        {},
        &conflictingSequenceNumber
    );

    EXPECT_EQ(ToSequenceNumber(5), conflictingSequenceNumber);

    co_await EnumerateExpectedRows(
        0,
        5,
        {
            {"key-1", "value-1", 5},
        }
    );

    co_await EnumerateExpectedRows(
        0,
        6,
        {
            {"key-1", "value-1", 5},
        }
    );

    co_await memoryTable->Join();
}

ASYNC_TEST_F(MemoryTableTests, AddRow_WriteConflict_from_Uncommitted_Row_that_commits)
{
    auto delayedOutcome = co_await AddRow(
        0,
        "key-1",
        "value-1",
        5,
        0,
        TransactionOutcome::Unknown
    );

    Phantom::Coroutines::async_scope<> scope;
    bool completed = false;
    scope.spawn([&]() -> reusable_task<>
    {
        std::optional<SequenceNumber> conflictingSequenceNumber;

        co_await AddRow(
            0,
            "key-1",
            "value-1-2",
            6,
            0,
            TransactionOutcome::Committed,
            {},
            &conflictingSequenceNumber
        );

        EXPECT_EQ(
            ToSequenceNumber(5),
            conflictingSequenceNumber);
        completed = true;
    });

    EXPECT_EQ(false, completed);
    delayedOutcome->BeginCommit(ToSequenceNumber(5));
    delayedOutcome->Complete();
    EXPECT_EQ(true, completed);

    co_await EnumerateExpectedRows(
        0,
        5,
        {
            {"key-1", "value-1", 5},
        }
    );

    co_await EnumerateExpectedRows(
        0,
        7,
        {
            {"key-1", "value-1", 5},
        }
    );

    co_await memoryTable->Join();
    co_await scope.join();
}

ASYNC_TEST_F(MemoryTableTests, AddRow_no_WriteConflict_from_Uncommitted_Row_that_aborts)
{
    auto delayedOutcome = co_await AddRow(
        1,
        "key-1",
        "value-1",
        5,
        0,
        TransactionOutcome::Unknown
    );

    Phantom::Coroutines::async_scope<> scope;
    bool completed = false;
    scope.spawn([&]() -> reusable_task<>
    {
        std::optional<SequenceNumber> conflictingSequenceNumber;

        co_await AddRow(
            0,
            "key-1",
            "value-1-2",
            7,
            0,
            TransactionOutcome::Committed,
            {},
            &conflictingSequenceNumber
        );

        EXPECT_FALSE(
            conflictingSequenceNumber);
        completed = true;
    });

    EXPECT_EQ(false, completed);
    delayedOutcome->Complete();
    EXPECT_EQ(true, completed);

    co_await EnumerateExpectedRows(
        0,
        5,
        {
        }
    );

    co_await EnumerateExpectedRows(
        0,
        7,
        {
            {"key-1", "value-1-2", 7},
        }
    );

    co_await memoryTable->Join();
}

ASYNC_TEST_F(MemoryTableTests, Enumerate_waits_for_transaction_resolution_abort)
{
    auto delayedTransactionOutcome = co_await AddRow(
        0,
        "key-1",
        "value-1",
        5,
        0,
        TransactionOutcome::Unknown
    );

    Phantom::Coroutines::async_scope<> scope;
    bool completed = false;
    scope.spawn([&]() -> reusable_task<>
    {
        co_await EnumerateExpectedRows(
            0,
            5,
            {
            });
        completed = true;
    });

    EXPECT_EQ(false, completed);
    delayedTransactionOutcome->Complete();
    EXPECT_EQ(true, completed);
    co_await scope.join();
    co_await memoryTable->Join();
}

ASYNC_TEST_F(MemoryTableTests, Enumerate_waits_for_transaction_resolution_commit)
{
    auto delayedTransactionOutcome = co_await AddRow(
        0,
        "key-1",
        "value-1",
        5,
        0,
        TransactionOutcome::Unknown
    );

    Phantom::Coroutines::async_scope<> scope;
    bool completed = false;
    scope.spawn([&]() -> reusable_task<>
    {
        co_await EnumerateExpectedRows(
            0,
            5,
            {
                { "key-1", "value-1", 5 }
            });
        completed = true;
    });

    EXPECT_EQ(false, completed);
    delayedTransactionOutcome->BeginCommit(ToSequenceNumber(5));
    delayedTransactionOutcome->Complete();
    EXPECT_EQ(true, completed);
    co_await scope.join();
    co_await memoryTable->Join();
}

ASYNC_TEST_F(MemoryTableTests, Succeed_to_add_conflicting_row_at_earlier_sequence_number_if_operation_aborted)
{
    co_await AddRow(
        0,
        "key-1",
        "value-1",
        5,
        0,
        TransactionOutcome::Aborted
    );

    co_await AddRow(
        0,
        "key-1",
        "value-1-2",
        4,
        0
    );

    co_await EnumerateExpectedRows(
        0,
        4,
        {
            {"key-1", "value-1-2", 4},
        }
    );

    co_await EnumerateExpectedRows(
        0,
        5,
        {
            {"key-1", "value-1-2", 4},
        }
    );

    co_await memoryTable->Join();
}

ASYNC_TEST_F(MemoryTableTests, Succeed_to_add_conflicting_row_at_later_sequence_number_if_operation_aborted)
{
    co_await AddRow(
        0,
        "key-1",
        "value-1",
        5,
        0,
        TransactionOutcome::Aborted
    );

    co_await AddRow(
        0,
        "key-1",
        "value-1-2",
        6,
        0
    );

    co_await EnumerateExpectedRows(
        0,
        5,
        {
        }
    );

    co_await EnumerateExpectedRows(
        0,
        6,
        {
            {"key-1", "value-1-2", 6},
        }
    );

    co_await memoryTable->Join();
}

ASYNC_TEST_F(MemoryTableTests, Succeed_to_add_conflicting_row_at_same_sequence_number_if_operation_aborted)
{
    co_await AddRow(
        0,
        "key-1",
        "value-1",
        5,
        0,
        TransactionOutcome::Aborted
    );

    co_await AddRow(
        0,
        "key-1",
        "value-1-2",
        5,
        0
    );

    co_await EnumerateExpectedRows(
        0,
        5,
        {
            {"key-1", "value-1-2", 5},
        }
    );

    co_await memoryTable->Join();
}

ASYNC_TEST_F(MemoryTableTests, Add_new_version_of_row_read_at_same_version_as_write)
{
    uint64_t version1 = 5;
    uint64_t version2 = 6;

    co_await AddRow(
        0,
        "key-1",
        "value-1",
        version1,
        0
    );

    co_await AddRow(
        0,
        "key-1",
        "value-1-2",
        version2,
        version1
    );

    co_await EnumerateExpectedRows(
        0,
        version1 - 1,
        {
        }
    );

    co_await EnumerateExpectedRows(
        0,
        version1,
        {
            {"key-1", "value-1", version1},
        }
    );

    co_await EnumerateExpectedRows(
        0,
        version2,
        {
            {"key-1", "value-1-2", version2},
        }
    );

    co_await EnumerateExpectedRows(
        0,
        version2 + 1,
        {
            {"key-1", "value-1-2", version2},
        }
    );

    co_await memoryTable->Join();
}

ASYNC_TEST_F(MemoryTableTests, Add_new_version_of_row_read_version_after_write_version)
{
    uint64_t version1 = 5;
    uint64_t version2 = 6;
    uint64_t version3 = 7;

    co_await AddRow(
        0,
        "key-1",
        "value-1",
        version1,
        0
    );

    co_await AddRow(
        0,
        "key-1",
        "value-1-2",
        version3,
        version2
    );

    co_await EnumerateExpectedRows(
        0,
        version1 - 1,
        {
        }
    );

    co_await EnumerateExpectedRows(
        0,
        version1,
        {
            {"key-1", "value-1", version1},
        }
    );

    co_await EnumerateExpectedRows(
        0,
        version2,
        {
            {"key-1", "value-1", version1},
        }
    );

    co_await EnumerateExpectedRows(
        0,
        version3,
        {
            {"key-1", "value-1-2", version3},
        }
    );

    co_await EnumerateExpectedRows(
        0,
        version3 + 1,
        {
            {"key-1", "value-1-2", version3},
        }
    );

    co_await memoryTable->Join();
}

ASYNC_TEST_F(MemoryTableTests, Enumerate_prefix_selects_rows_matching_prefix_and_ignores_rows_not_matching_prefix)
{
    schema = std::make_shared<Schema>(
            KeySchema{ TestKey::descriptor() },
            ValueSchema{ StringValue::descriptor() });

    keyComparer = std::make_shared<ProtocolBuffersValueComparer>(
        TestKey::descriptor());

    std::shared_ptr<ValueComparer> valueComparer(
        std::make_shared<ProtocolBuffersValueComparer>(
            StringValue::descriptor()));

    memoryTable = std::make_shared<MemoryTable>(
        schema,
        keyComparer);

    // Outside prefix
    TestKey testKey_1;
    StringValue testValue_1;
    // Inside prefix
    TestKey testKey_2_1;
    StringValue testValue_2_1;
    TestKey testKey_2_2;
    StringValue testValue_2_2;
    // Outside prefix
    TestKey testKey_3;
    StringValue testValue_3;

    testKey_1.set_sint32_value(1);
    testValue_1.set_value("1");
    
    testKey_2_1.set_sint32_value(2);
    testKey_2_1.set_fixed32_value(1);
    testValue_2_1.set_value("2_1");
    
    testKey_2_2.set_sint32_value(2);
    testKey_2_2.set_fixed64_value(2);
    testValue_2_2.set_value("2_2");
    
    testKey_3.set_sint32_value(3);
    testValue_3.set_value("3");

    std::optional<SequenceNumber> conflictingSequenceNumber;

    co_await AddRow(
        0,
        testKey_1,
        testValue_1,
        1,
        1,
        TransactionOutcome::Committed,
        {},
        &conflictingSequenceNumber);

    EXPECT_FALSE(conflictingSequenceNumber);

    co_await AddRow(
        0,
        testKey_2_1,
        testValue_2_1,
        1,
        1,
        TransactionOutcome::Committed,
        {},
        &conflictingSequenceNumber);

    EXPECT_FALSE(conflictingSequenceNumber);

    co_await AddRow(
        0,
        testKey_2_2,
        testValue_2_2,
        1,
        1,
        TransactionOutcome::Committed,
        {},
        &conflictingSequenceNumber);

    EXPECT_FALSE(conflictingSequenceNumber);

    co_await AddRow(
        0,
        testKey_3,
        testValue_3,
        1,
        1,
        TransactionOutcome::Committed,
        {},
        &conflictingSequenceNumber);

    EXPECT_FALSE(conflictingSequenceNumber);

    KeyRangeEnd keyRangeLow
    {
        // Use testKey_2_2 to enumerate in order to verify that prefix
        // logic is used.
        .Key = ProtoValue{ &testKey_2_2 }.pack(),
        .Inclusivity = Inclusivity::Inclusive,
        .LastFieldId = 3
    };

    auto enumeration = memoryTable->Enumerate(
        nullptr,
        ToSequenceNumber(10),
        keyRangeLow,
        keyRangeLow);

    auto iterator = co_await enumeration.begin();
    EXPECT_FALSE(iterator == enumeration.end());
    EXPECT_TRUE(keyComparer->Equals(ProtoValue{ &testKey_2_1 }.pack(), iterator->Key));
    EXPECT_TRUE(valueComparer->Equals(ProtoValue{ &testValue_2_1 }.pack(), iterator->Value));
    co_await ++iterator;
    EXPECT_FALSE(iterator == enumeration.end());
    EXPECT_TRUE(keyComparer->Equals(ProtoValue{ &testKey_2_2 }.pack(), iterator->Key));
    EXPECT_TRUE(valueComparer->Equals(ProtoValue{ &testValue_2_2 }.pack(), iterator->Value));
    co_await ++iterator;
    EXPECT_TRUE(iterator == enumeration.end());
}

ASYNC_TEST_F(MemoryTableTests, CheckForWriteConflicts_returns_null_if_key_not_present)
{
    auto result = co_await CheckForWriteConflict(
        nullptr,
        "key-1",
        ToSequenceNumber(1)
    );

    EXPECT_EQ(std::nullopt, result);
}

ASYNC_TEST_F(MemoryTableTests, CheckForWriteConflicts_returns_null_if_later_sequence_key_was_aborted)
{
    co_await AddRow(
        0,
        "key-1",
        "value-1",
        5,
        0,
        TransactionOutcome::Aborted
    );
    
    auto result = co_await CheckForWriteConflict(
        nullptr,
        "key-1",
        ToSequenceNumber(1)
    );

    EXPECT_EQ(std::nullopt, result);
}

ASYNC_TEST_F(MemoryTableTests, CheckForWriteConflicts_returns_sequence_number_if_later_sequence_key_was_committed)
{
    co_await AddRow(
        10,
        "key-1",
        "value-1",
        5,
        0,
        TransactionOutcome::Committed
    );
    
    auto result = co_await CheckForWriteConflict(
        nullptr,
        "key-1",
        ToSequenceNumber(1)
    );

    EXPECT_EQ(ToSequenceNumber(5), result);
}

ASYNC_TEST_F(MemoryTableTests, CheckForWriteConflicts_returns_sequence_number_if_same_sequence_key_was_committed)
{
    co_await AddRow(
        10,
        "key-1",
        "value-1",
        10,
        0,
        TransactionOutcome::Committed
    );
    
    auto result = co_await CheckForWriteConflict(
        nullptr,
        "key-1",
        ToSequenceNumber(10)
    );

    EXPECT_EQ(ToSequenceNumber(10), result);
}

ASYNC_TEST_F(MemoryTableTests, CheckForWriteConflicts_returns_null_if_earlier_sequence_key_was_committed)
{
    co_await AddRow(
        10,
        "key-1",
        "value-1",
        10,
        0,
        TransactionOutcome::Committed
    );
    
    auto result = co_await CheckForWriteConflict(
        nullptr,
        "key-1",
        ToSequenceNumber(11)
    );

    EXPECT_EQ(std::nullopt, result);
}

ASYNC_TEST_F(MemoryTableTests, CheckForWriteConflicts_returns_null_if_earlier_sequence_key_was_aborted)
{
    co_await AddRow(
        10,
        "key-1",
        "value-1",
        10,
        0,
        TransactionOutcome::Committed
    );
    
    auto result = co_await CheckForWriteConflict(
        nullptr,
        "key-1",
        ToSequenceNumber(11)
    );

    EXPECT_EQ(std::nullopt, result);
}

}