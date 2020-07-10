#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/MemoryTableImpl.h"
#include "ProtoStoreTest.pb.h"

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
    task<> AddRow(
        std::string key,
        std::string value,
        std::uint64_t writeSequenceNumber,
        std::uint64_t readSequenceNumber)
    {
        StringKey rowKey;
        rowKey.set_value(key);
        StringValue rowValue;
        rowValue.set_value(value);

        MemoryTableRow row
        {
            .Key = copy_unique(rowKey),
            .SequenceNumber = static_cast<SequenceNumber>(writeSequenceNumber),
            .Value = copy_unique(rowValue),
            .TransactionId = nullptr,
        };

        co_await memoryTable.AddRow(
            static_cast<SequenceNumber>(readSequenceNumber),
            row);
    }
};

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
        value2.set_value("value-1");

        MemoryTableRow row2
        {
            copy_unique(key2),
            static_cast<SequenceNumber>(6),
            copy_unique(value2),
            nullptr,
        };

        ASSERT_THROW(
            co_await memoryTable.AddRow(
                SequenceNumber::Earliest,
                row2),
            WriteConflict);

        ASSERT_EQ("key-1", static_cast<StringKey*>(row2.Key.get())->value());
        ASSERT_EQ("value-1", static_cast<StringValue*>(row2.Value.get())->value());
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
        value2.set_value("value-1");

        MemoryTableRow row2
        {
            copy_unique(key2),
            static_cast<SequenceNumber>(5),
            copy_unique(value2),
            nullptr,
        };

        ASSERT_THROW(
            co_await memoryTable.AddRow(
                static_cast<SequenceNumber>(7),
                row2),
            WriteConflict);

        ASSERT_EQ("key-1", static_cast<StringKey*>(row2.Key.get())->value());
        ASSERT_EQ("value-1", static_cast<StringValue*>(row2.Value.get())->value());
    });
}

TEST_F(MemoryTableTests, Add_new_version_of_row_read_at_same_version_as_write)
{
    run_async([&]()->task<>
    {
        auto version1 = 5;
        auto version2 = 6;

        co_await AddRow(
            "key-1",
            "value-1",
            version1,
            0
        );

        co_await AddRow(
            "key-1",
            "value-1",
            version2,
            version1
        );
    });
}

TEST_F(MemoryTableTests, Add_new_version_of_row_read_version_after_write_version)
{
    run_async([&]()->task<>
    {
        auto version1 = 5;
        auto version2 = 6;
        auto version3 = 7;

        co_await AddRow(
            "key-1",
            "value-1",
            version1,
            0
        );

        co_await AddRow(
            "key-1",
            "value-1",
            version3,
            version2
        );
    });
}

}