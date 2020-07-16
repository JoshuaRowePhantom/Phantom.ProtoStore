#include "StandardIncludes.h"
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
        OperationOutcome outcome = OperationOutcome::Committed)
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

        for co_await(auto row : enumeration)
        {
            storedRows.push_back(
                {
                    .Key = static_cast<const StringKey*>(row->Key.get())->value(),
                    .Value = static_cast<const StringKey*>(row->Value.get())->value(),
                    .SequenceNumber = ToUint64(row->WriteSequenceNumber),
                }
            );
        }

        ASSERT_EQ(
            expectedRows,
            storedRows);

        co_await memoryTable.Join();
    }
};

TEST_F(MemoryTableTests, Can_add_and_enumerate_one_row)
{
    run_async([&]()->task<>
    {
        co_await AddRow(
            "key-1",
            "value-1",
            5,
            0
        );

        co_await EnumerateExpectedRows(
            5,
            {
                {"key-1", "value-1", 5},
            }
        );

        co_await memoryTable.Join();
    });
}

TEST_F(MemoryTableTests, Can_add_distinct_rows)
{
    run_async([&]()->task<>
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
    });
}

TEST_F(MemoryTableTests, Skips_aborted_rows)
{
    run_async([&]()->task<>
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
    });
}

TEST_F(MemoryTableTests, Fail_to_add_write_conflict_from_ReadSequenceNumber)
{
    run_async([&]()->task<>
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

        ASSERT_THROW(
            co_await memoryTable.AddRow(
                SequenceNumber::Earliest,
                row2,
                WithOutcome(
                    OperationOutcome::Committed,
                    6)),
            WriteConflict);

        ASSERT_EQ("key-1", static_cast<const StringKey*>(row2.Key.get())->value());
        ASSERT_EQ("value-1-2", static_cast<const StringValue*>(row2.Value.get())->value());

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
    });
}

TEST_F(MemoryTableTests, Fail_to_add_write_conflict_from_Row)
{
    run_async([&]()->task<>
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

        ASSERT_THROW(
            co_await memoryTable.AddRow(
                ToSequenceNumber(7),
                row2,
                WithOutcome(OperationOutcome::Committed, 7)),
            WriteConflict);

        ASSERT_EQ("key-1", static_cast<const StringKey*>(row2.Key.get())->value());
        ASSERT_EQ("value-1-2", static_cast<const StringValue*>(row2.Value.get())->value());

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
    });
}

TEST_F(MemoryTableTests, Succeed_to_add_conflicting_row_at_same_sequence_number_if_operation_aborted)
{
    run_async([&]()->task<>
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
    });
}

TEST_F(MemoryTableTests, Add_new_version_of_row_read_at_same_version_as_write)
{
    run_async([&]()->task<>
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
    });
}

TEST_F(MemoryTableTests, Add_new_version_of_row_read_version_after_write_version)
{
    run_async([&]()->task<>
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
    });
}

}