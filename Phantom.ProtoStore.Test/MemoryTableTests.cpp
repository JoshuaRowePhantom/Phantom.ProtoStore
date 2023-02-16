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
    KeyComparer keyComparer;
    MemoryTable memoryTable;

public:
    MemoryTableTests()
        : 
        keyComparer(
            StringKey::descriptor()),
        memoryTable(
            &keyComparer)
    {}

protected:
    MemoryTableOperationOutcomeTask WithOutcome(
        OperationOutcome outcome,
        uint64_t writeSequenceNumber)
    {
        co_return MemoryTableOperationOutcome
        {
            .Outcome = outcome,
            .WriteSequenceNumber = ToSequenceNumber(writeSequenceNumber),
        };
    }

    task<> AddRow(
        string key,
        string value,
        uint64_t writeSequenceNumber,
        uint64_t readSequenceNumber,
        OperationOutcome outcome = OperationOutcome::Committed,
        std::optional<TransactionId> transactionId = {})
    {
        StringKey rowKey;
        rowKey.set_value(key);
        StringValue rowValue;
        rowValue.set_value(value);

        MemoryTableRow row
        {
            .Key = copy_unique(rowKey),
            .WriteSequenceNumber = ToSequenceNumber(writeSequenceNumber),
            .Value = copy_unique(rowValue),
            .TransactionId = transactionId,
        };

        co_await memoryTable.AddRow(
            ToSequenceNumber(readSequenceNumber),
            row,
            WithOutcome(outcome, writeSequenceNumber));
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
        uint64_t readSequenceNumber,
        vector<ExpectedRow> expectedRows,
        optional<string> keyLow = optional<string>(),
        optional<string> keyHigh = optional<string>(),
        Inclusivity keyLowInclusivity = Inclusivity::Inclusive,
        Inclusivity keyHighInclusivity = Inclusivity::Exclusive
    )
    {
        StringKey keyLowMessage;
        StringKey* keyLowMessagePointer = nullptr;
        if (keyLow.has_value())
        {
            keyLowMessage.set_value(
                keyLow.value());
            keyLowMessagePointer = &keyLowMessage;
        }

        StringKey keyHighMessage;
        StringKey* keyHighMessagePointer = nullptr;
        if (keyHigh.has_value())
        {
            keyHighMessage.set_value(
                keyHigh.value());
            keyHighMessagePointer = &keyHighMessage;
        }

        auto enumeration = memoryTable.Enumerate(
            ToSequenceNumber(readSequenceNumber),
            {
                .Key = keyLowMessagePointer,
                .Inclusivity = keyLowInclusivity,
            },
            {
                .Key = keyHighMessagePointer,
                .Inclusivity = keyHighInclusivity
            });

        vector<ExpectedRow> storedRows;

        for (auto iterator = co_await enumeration.begin();
            iterator != enumeration.end();
            co_await ++iterator)
        {
            auto& row = *iterator;

            storedRows.push_back(
                {
                    .Key = static_cast<const StringKey*>(row.Key)->value(),
                    .Value = static_cast<const StringKey*>(row.Value)->value(),
                    .SequenceNumber = ToUint64(row.WriteSequenceNumber),
                    .TransactionId = row.TransactionId ? std::optional { *row.TransactionId } : std::optional<TransactionId> { },
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
        "key-1",
        "value-1",
        5,
        0,
        OperationOutcome::Committed,
        "transaction id"
    );

    co_await EnumerateExpectedRows(
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
        "key-1",
        "value-1",
        5,
        0
    );

    co_await AddRow(
        "key-2",
        "value-2",
        5,
        0
    );

    co_await EnumerateExpectedRows(
        5,
        {
            {"key-1", "value-1", 5},
            {"key-2", "value-2", 5},
        }
    );

    co_await memoryTable.Join();
}

ASYNC_TEST_F(MemoryTableTests, Skips_aborted_rows)
{
    co_await AddRow(
        "key-1",
        "value-1",
        5,
        0
    );

    co_await AddRow(
        "key-2",
        "value-2",
        5,
        0,
        OperationOutcome::Aborted
    );

    co_await EnumerateExpectedRows(
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
        "key-1",
        "value-1",
        5,
        0
    );

    StringKey key2;
    key2.set_value("key-1");
    StringValue value2;
    value2.set_value("value-1-2");

    MemoryTableRow row2
    {
        .Key = copy_unique(key2),
        .WriteSequenceNumber = ToSequenceNumber(6),
        .Value = copy_unique(value2),
    };

    auto result =
        co_await memoryTable.AddRow(
            SequenceNumber::Earliest,
            row2,
            WithOutcome(
                OperationOutcome::Committed,
                6));

    EXPECT_EQ(ToSequenceNumber(5), result);
    EXPECT_EQ("key-1", static_cast<const StringKey*>(row2.Key.get())->value());
    EXPECT_EQ("value-1-2", static_cast<const StringValue*>(row2.Value.get())->value());

    co_await EnumerateExpectedRows(
        5,
        {
            {"key-1", "value-1", 5},
        }
    );

    co_await EnumerateExpectedRows(
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
        "key-1",
        "value-1",
        5,
        0
    );

    StringKey key2;
    key2.set_value("key-1");
    StringValue value2;
    value2.set_value("value-1-2");

    MemoryTableRow row2
    {
        .Key = copy_unique(key2),
        .WriteSequenceNumber = ToSequenceNumber(5),
        .Value = copy_unique(value2),
    };

    auto result =
        co_await memoryTable.AddRow(
            ToSequenceNumber(7),
            row2,
            WithOutcome(OperationOutcome::Unknown, 7));

    EXPECT_EQ(ToSequenceNumber(5), result);
    EXPECT_EQ("key-1", static_cast<const StringKey*>(row2.Key.get())->value());
    EXPECT_EQ("value-1-2", static_cast<const StringValue*>(row2.Value.get())->value());

    co_await EnumerateExpectedRows(
        5,
        {
            {"key-1", "value-1", 5},
        }
    );

    co_await EnumerateExpectedRows(
        6,
        {
            {"key-1", "value-1", 5},
        }
    );

    co_await memoryTable.Join();
}

ASYNC_TEST_F(MemoryTableTests, Fail_to_add_write_conflict_from_Uncommitted_Row)
{
    co_await AddRow(
        "key-1",
        "value-1",
        5,
        0,
        OperationOutcome::Unknown
    );

    StringKey key2;
    key2.set_value("key-1");
    StringValue value2;
    value2.set_value("value-1-2");

    MemoryTableRow row2
    {
        .Key = copy_unique(key2),
        .WriteSequenceNumber = ToSequenceNumber(5),
        .Value = copy_unique(value2),
    };

    auto result =
        co_await memoryTable.AddRow(
            ToSequenceNumber(7),
            row2,
            WithOutcome(OperationOutcome::Committed, 7));

    EXPECT_EQ(
        ToSequenceNumber(5),
        result);

    EXPECT_EQ("key-1", static_cast<const StringKey*>(row2.Key.get())->value());
    EXPECT_EQ("value-1-2", static_cast<const StringValue*>(row2.Value.get())->value());

    co_await EnumerateExpectedRows(
        5,
        {
            {"key-1", "value-1", 5},
        }
    );

    co_await EnumerateExpectedRows(
        6,
        {
            {"key-1", "value-1", 5},
        }
    );

    co_await memoryTable.Join();
}

ASYNC_TEST_F(MemoryTableTests, Fail_to_read_conflict_from_Uncommitted_Row)
{
    co_await AddRow(
        "key-1",
        "value-1",
        5,
        0,
        OperationOutcome::Unknown
    );

    StringKey key2;
    key2.set_value("key-1");
    StringValue value2;
    value2.set_value("value-1-2");

    co_await EnumerateExpectedRows(
        5,
        {
            {"key-1", "value-1", 5},
        }
    );

    co_await EnumerateExpectedRows(
        6,
        {
            {"key-1", "value-1", 5},
        }
    );

    co_await memoryTable.Join();
}

ASYNC_TEST_F(MemoryTableTests, Succeed_to_add_conflicting_row_at_earlier_sequence_number_if_operation_aborted)
{
    co_await AddRow(
        "key-1",
        "value-1",
        5,
        0,
        OperationOutcome::Aborted
    );

    co_await AddRow(
        "key-1",
        "value-1-2",
        4,
        0
    );

    co_await EnumerateExpectedRows(
        4,
        {
            {"key-1", "value-1-2", 4},
        }
    );

    co_await EnumerateExpectedRows(
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
        "key-1",
        "value-1",
        5,
        0,
        OperationOutcome::Aborted
    );

    co_await AddRow(
        "key-1",
        "value-1-2",
        6,
        0
    );

    co_await EnumerateExpectedRows(
        5,
        {
        }
    );

    co_await EnumerateExpectedRows(
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
        "key-1",
        "value-1",
        5,
        0,
        OperationOutcome::Aborted
    );

    co_await AddRow(
        "key-1",
        "value-1-2",
        5,
        0
    );

    co_await EnumerateExpectedRows(
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
        "key-1",
        "value-1",
        version1,
        0
    );

    co_await AddRow(
        "key-1",
        "value-1-2",
        version2,
        version1
    );

    co_await EnumerateExpectedRows(
        version1 - 1,
        {
        }
    );

    co_await EnumerateExpectedRows(
        version1,
        {
            {"key-1", "value-1", version1},
        }
    );

    co_await EnumerateExpectedRows(
        version2,
        {
            {"key-1", "value-1-2", version2},
        }
    );

    co_await EnumerateExpectedRows(
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
        "key-1",
        "value-1",
        version1,
        0
    );

    co_await AddRow(
        "key-1",
        "value-1-2",
        version3,
        version2
    );

    co_await EnumerateExpectedRows(
        version1 - 1,
        {
        }
    );

    co_await EnumerateExpectedRows(
        version1,
        {
            {"key-1", "value-1", version1},
        }
    );

    co_await EnumerateExpectedRows(
        version2,
        {
            {"key-1", "value-1", version1},
        }
    );

    co_await EnumerateExpectedRows(
        version3,
        {
            {"key-1", "value-1-2", version3},
        }
    );

    co_await EnumerateExpectedRows(
        version3 + 1,
        {
            {"key-1", "value-1-2", version3},
        }
    );

    co_await memoryTable.Join();
}

ASYNC_TEST(DelayedMemoryTableOperationOutcomeTests, Commit_before_GetOutcome_has_GetOutcome_return_committed_sequence_number)
{
    DelayedMemoryTableOperationOutcome outcome(5);
    outcome.Commit(ToSequenceNumber(7));
    auto result = co_await outcome.GetOutcome();
    EXPECT_EQ(
        result,
        (MemoryTableOperationOutcome 
        {
            OperationOutcome::Committed,
            ToSequenceNumber(7),
        }));
}

ASYNC_TEST(DelayedMemoryTableOperationOutcomeTests, Commit_after_GetOutcome_has_GetOutcome_return_committed_sequence_number)
{
    DelayedMemoryTableOperationOutcome outcome(5);
    Phantom::Coroutines::async_scope<> scope;
    bool completed = false;

    scope.spawn([&]() -> reusable_task<>
    {
        auto result = co_await outcome.GetOutcome();
        completed = true;
        EXPECT_EQ(
            result,
            (MemoryTableOperationOutcome
                {
                    OperationOutcome::Committed,
                    ToSequenceNumber(7),
                }));
    });

    EXPECT_EQ(false, completed);
    outcome.Commit(ToSequenceNumber(7));
    EXPECT_EQ(true, completed);

    co_await scope.join();
}

ASYNC_TEST(DelayedMemoryTableOperationOutcomeTests, Abort_before_GetOutcome_has_GetOutcome_return_aborted)
{
    DelayedMemoryTableOperationOutcome outcome(5);
    outcome.Abort();
    auto result = co_await outcome.GetOutcome();
    EXPECT_EQ(
        result,
        (MemoryTableOperationOutcome
            {
                OperationOutcome::Aborted,
                ToSequenceNumber(0),
            }));
}

ASYNC_TEST(DelayedMemoryTableOperationOutcomeTests, Abort_after_GetOutcome_has_GetOutcome_return_aborted)
{
    DelayedMemoryTableOperationOutcome outcome(5);
    Phantom::Coroutines::async_scope<> scope;
    bool completed = false;

    scope.spawn([&]() -> reusable_task<>
    {
        auto result = co_await outcome.GetOutcome();
    completed = true;
    EXPECT_EQ(
        result,
        (MemoryTableOperationOutcome
            {
                OperationOutcome::Aborted,
                ToSequenceNumber(0),
            }));
    });

    EXPECT_EQ(false, completed);
    outcome.Abort();
    EXPECT_EQ(true, completed);

    co_await scope.join();
}

ASYNC_TEST(DelayedMemoryTableOperationOutcomeTests, Resolve_earlier_transaction_sequence_number_before_GetOutcome_has_GetOutcome_return_aborted)
{
    DelayedMemoryTableOperationOutcome outcome(5);
    auto result = co_await outcome.Resolve(4);

    EXPECT_EQ(
        result,
        (MemoryTableOperationOutcome
            {
                OperationOutcome::Aborted,
                ToSequenceNumber(0),
            }));

    result = co_await outcome.GetOutcome();

    EXPECT_EQ(
        result,
        (MemoryTableOperationOutcome
            {
                OperationOutcome::Aborted,
                ToSequenceNumber(0),
            }));
}

ASYNC_TEST(DelayedMemoryTableOperationOutcomeTests, Resolve_earlier_transaction_sequence_number_after_GetOutcome_has_GetOutcome_return_aborted)
{
    DelayedMemoryTableOperationOutcome outcome(5);
    Phantom::Coroutines::async_scope<> scope;
    bool completed = false;

    scope.spawn([&]() -> reusable_task<>
    {
        auto result = co_await outcome.GetOutcome();
        completed = true;
        EXPECT_EQ(
            result,
            (MemoryTableOperationOutcome
                {
                    OperationOutcome::Aborted,
                    ToSequenceNumber(0),
                }));
    });

    EXPECT_EQ(false, completed);
    auto result = co_await outcome.Resolve(4);
    EXPECT_EQ(
        result,
        (MemoryTableOperationOutcome
            {
                OperationOutcome::Aborted,
                ToSequenceNumber(0),
            }));
    EXPECT_EQ(true, completed);

    co_await scope.join();
}

ASYNC_TEST(DelayedMemoryTableOperationOutcomeTests, Resolve_later_transaction_sequence_number_before_GetOutcome_waits_for_Commit)
{
    DelayedMemoryTableOperationOutcome outcome(5);
    Phantom::Coroutines::async_scope<> scope;

    MemoryTableOperationOutcome expectedOutcome
    {
        OperationOutcome::Committed,
        ToSequenceNumber(7),
    };

    bool resolveCompleted = false;
    scope.spawn([&]() -> reusable_task<>
    {
        auto result = co_await outcome.Resolve(6);
        EXPECT_EQ(
            result,
            expectedOutcome);
        resolveCompleted = true;
    });

    EXPECT_EQ(resolveCompleted, false);

    auto result = outcome.Commit(ToSequenceNumber(7));
    EXPECT_EQ(resolveCompleted, true);
    EXPECT_EQ(
        result,
        expectedOutcome);

    result = co_await outcome.GetOutcome();

    EXPECT_EQ(
        result,
        expectedOutcome);

    co_await scope.join();
}

ASYNC_TEST(DelayedMemoryTableOperationOutcomeTests, Resolve_later_transaction_sequence_number_after_GetOutcome_waits_for_Commit)
{
    DelayedMemoryTableOperationOutcome outcome(5);
    Phantom::Coroutines::async_scope<> scope;

    MemoryTableOperationOutcome expectedOutcome
    {
        OperationOutcome::Committed,
        ToSequenceNumber(7),
    };

    bool resolveCompleted = false;
    scope.spawn([&]() -> reusable_task<>
    {
        auto result = co_await outcome.Resolve(6);
        EXPECT_EQ(
            result,
            expectedOutcome);
        resolveCompleted = true;
    });

    bool getOutcomeComplete = false;
    scope.spawn([&]() -> reusable_task<>
    {
        auto result = co_await outcome.GetOutcome();
        EXPECT_EQ(
            result,
            expectedOutcome);
        getOutcomeComplete = true;
    });

    EXPECT_EQ(resolveCompleted, false);
    EXPECT_EQ(getOutcomeComplete, false);

    auto result = outcome.Commit(ToSequenceNumber(7));
    EXPECT_EQ(resolveCompleted, true);
    EXPECT_EQ(getOutcomeComplete, true);
    EXPECT_EQ(
        result,
        expectedOutcome);

    co_await scope.join();
}

ASYNC_TEST(DelayedMemoryTableOperationOutcomeTests, Resolve_later_transaction_sequence_number_before_GetOutcome_waits_for_Abort)
{
    DelayedMemoryTableOperationOutcome outcome(5);
    Phantom::Coroutines::async_scope<> scope;

    MemoryTableOperationOutcome expectedOutcome
    {
        OperationOutcome::Aborted,
        ToSequenceNumber(0),
    };

    bool resolveCompleted = false;
    scope.spawn([&]() -> reusable_task<>
    {
        auto result = co_await outcome.Resolve(6);
        EXPECT_EQ(
            result,
            expectedOutcome);
        resolveCompleted = true;
    });

    EXPECT_EQ(resolveCompleted, false);

    outcome.Abort();
    EXPECT_EQ(resolveCompleted, true);

    auto result = co_await outcome.GetOutcome();

    EXPECT_EQ(
        result,
        expectedOutcome);

    co_await scope.join();
}

ASYNC_TEST(DelayedMemoryTableOperationOutcomeTests, Resolve_later_transaction_sequence_number_after_GetOutcome_waits_for_Abort)
{
    DelayedMemoryTableOperationOutcome outcome(5);
    Phantom::Coroutines::async_scope<> scope;

    MemoryTableOperationOutcome expectedOutcome
    {
        OperationOutcome::Aborted,
        ToSequenceNumber(0),
    };

    bool resolveCompleted = false;
    scope.spawn([&]() -> reusable_task<>
    {
        auto result = co_await outcome.Resolve(6);
        EXPECT_EQ(
            result,
            expectedOutcome);
        resolveCompleted = true;
    });

    bool getOutcomeComplete = false;
    scope.spawn([&]() -> reusable_task<>
    {
        auto result = co_await outcome.GetOutcome();
        EXPECT_EQ(
            result,
            expectedOutcome);
        getOutcomeComplete = true;
    });

    EXPECT_EQ(resolveCompleted, false);
    EXPECT_EQ(getOutcomeComplete, false);

    outcome.Abort();
    EXPECT_EQ(resolveCompleted, true);
    EXPECT_EQ(getOutcomeComplete, true);

    co_await scope.join();
}

ASYNC_TEST(DelayedMemoryTableOperationOutcomeTests, Resolve_later_transaction_sequence_number_before_GetOutcome_waits_for_abort_from_earlier_Resolve)
{
    DelayedMemoryTableOperationOutcome outcome(5);
    Phantom::Coroutines::async_scope<> scope;

    MemoryTableOperationOutcome expectedOutcome
    {
        OperationOutcome::Aborted,
        ToSequenceNumber(0),
    };

    bool resolveCompleted = false;
    scope.spawn([&]() -> reusable_task<>
    {
        auto result = co_await outcome.Resolve(6);
        EXPECT_EQ(
            result,
            expectedOutcome);
        resolveCompleted = true;
    });

    EXPECT_EQ(resolveCompleted, false);

    auto result = co_await outcome.Resolve(4);
    EXPECT_EQ(
        result,
        expectedOutcome);

    EXPECT_EQ(resolveCompleted, true);

    result = co_await outcome.GetOutcome();
    EXPECT_EQ(
        result,
        expectedOutcome);

    co_await scope.join();
}

ASYNC_TEST(DelayedMemoryTableOperationOutcomeTests, Resolve_later_transaction_sequence_number_after_GetOutcome_waits_for_abort_from_earlier_Resolve)
{
    DelayedMemoryTableOperationOutcome outcome(5);
    Phantom::Coroutines::async_scope<> scope;

    MemoryTableOperationOutcome expectedOutcome
    {
        OperationOutcome::Aborted,
        ToSequenceNumber(0),
    };

    bool resolveCompleted = false;
    scope.spawn([&]() -> reusable_task<>
    {
        auto result = co_await outcome.Resolve(6);
        EXPECT_EQ(
            result,
            expectedOutcome);
        resolveCompleted = true;
    });

    bool getOutcomeComplete = false;
    scope.spawn([&]() -> reusable_task<>
    {
        auto result = co_await outcome.GetOutcome();
        EXPECT_EQ(
            result,
            expectedOutcome);
        getOutcomeComplete = true;
    });

    EXPECT_EQ(resolveCompleted, false);
    EXPECT_EQ(getOutcomeComplete, false);

    auto result = co_await outcome.Resolve(4);
    EXPECT_EQ(
        result,
        expectedOutcome);

    EXPECT_EQ(resolveCompleted, true);
    EXPECT_EQ(getOutcomeComplete, true);

    co_await scope.join();
}

}