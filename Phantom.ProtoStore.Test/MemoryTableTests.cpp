#include "StandardIncludes.h"
#include "Phantom.Coroutines/async_scope.h"
#include "Phantom.ProtoStore/src/MemoryTableImpl.h"
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
protected:
    std::shared_ptr<Schema> schema;
    std::shared_ptr<ProtoKeyComparer> keyComparer;
    MemoryTable memoryTable;

public:
    MemoryTableTests()
        : 
        schema(
            std::make_shared<Schema>(
                KeySchema{ StringKey::descriptor() },
                ValueSchema{ StringValue::descriptor() }
            )),
        keyComparer(
            std::make_shared<ProtoKeyComparer>(
                StringKey::descriptor())),
        memoryTable(
            schema,
            keyComparer)
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

        auto rowKeyProto = ProtoValue(&rowKey).pack();
        auto rowValueProto = ProtoValue (&rowValue).pack();

        FlatBuffers::LoggedRowWriteT loggedRowWrite;
        if (transactionId)
        {
            loggedRowWrite.distributed_transaction_id = ToDataValue(
                get_byte_span(*transactionId));
        }
        loggedRowWrite.key = ToDataValue(
            rowKeyProto.as_protocol_buffer_bytes_if());

        loggedRowWrite.value = ToDataValue(
            rowValueProto.as_protocol_buffer_bytes_if());
        loggedRowWrite.sequence_number = writeSequenceNumber;

        FlatMessage loggedRowWriteMessage{ &loggedRowWrite };

        auto delayedOutcome = WithOutcome(transactionSequenceNumber, outcome, writeSequenceNumber);

        auto result = co_await memoryTable.AddRow(
            ToSequenceNumber(readSequenceNumber),
            std::move(loggedRowWriteMessage),
            delayedOutcome);

        if (conflictingSequenceNumber)
        {
            *conflictingSequenceNumber = result;
        }

        co_return delayedOutcome;
    }

    struct ExpectedRow
    {
        string Key;
        string Value;
        uint64_t SequenceNumber;
        std::optional<TransactionId> TransactionId;

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

        auto enumeration = memoryTable.Enumerate(
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
                    .TransactionId = row.TransactionId->Payload.data()
                        ? std::optional { std::string {
                            get_char_span(row.TransactionId->Payload).begin(),
                            get_char_span(row.TransactionId->Payload).end()
                            } }
                        : std::optional<TransactionId> {},
                }
            );
        }

        EXPECT_EQ(
            expectedRows,
            storedRows);

        co_await memoryTable.Join();
    }
};

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

    co_await memoryTable.Join();
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

    co_await memoryTable.Join();
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

    co_await memoryTable.Join();
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

    co_await memoryTable.Join();
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

    co_await memoryTable.Join();
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

    co_await memoryTable.Join();
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

    co_await memoryTable.Join();
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
    co_await memoryTable.Join();
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
    co_await memoryTable.Join();
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

    co_await memoryTable.Join();
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

    co_await memoryTable.Join();
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

    co_await memoryTable.Join();
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

    co_await memoryTable.Join();
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

    co_await memoryTable.Join();
}

}