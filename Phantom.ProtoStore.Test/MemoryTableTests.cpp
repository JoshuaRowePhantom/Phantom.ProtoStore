#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/MemoryTableImpl.h"
#include "ProtoStoreTest.pb.h"

namespace Phantom::ProtoStore
{
TEST(MemoryTableTests, Can_add_distinct_rows)
{
    run_async([]()->task<>
    {
        KeyComparer keyComparer(
            StringKey::descriptor());

        MemoryTable memoryTable(
            &keyComparer);

        StringKey key1;
        key1.set_value("key-1");
        StringValue value1;
        key1.set_value("value-1");

        MemoryTableRow row1
        {
            copy_unique(key1),
            static_cast<SequenceNumber>(5),
            copy_unique(value1),
            nullptr,
        };

        co_await memoryTable.AddRow(
            SequenceNumber::Earliest,
            row1);


        StringKey key2;
        key2.set_value("key-2");
        StringValue value2;
        key2.set_value("value-2");

        MemoryTableRow row2
        {
            copy_unique(key2),
            static_cast<SequenceNumber>(5),
            copy_unique(value2),
            nullptr,
        };

        co_await memoryTable.AddRow(
            SequenceNumber::Earliest,
            row2);
    });
}

TEST(MemoryTableTests, Fail_to_add_write_conflict_from_ReadSequenceNumber)
{
    run_async([]()->task<>
    {
        KeyComparer keyComparer(
            StringKey::descriptor());

        MemoryTable memoryTable(
            &keyComparer);

        StringKey key1;
        key1.set_value("key-1");
        StringValue value1;
        key1.set_value("value-1");

        MemoryTableRow row1
        {
            copy_unique(key1),
            static_cast<SequenceNumber>(5),
            copy_unique(value1),
            nullptr,
        };

        co_await memoryTable.AddRow(
            SequenceNumber::Earliest,
            row1);

        StringKey key2;
        key2.set_value("key-1");
        StringValue value2;
        key2.set_value("value-1");

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
    });
}

TEST(MemoryTableTests, Fail_to_add_write_conflict_from_Row)
{
    run_async([]()->task<>
    {
        KeyComparer keyComparer(
            StringKey::descriptor());

        MemoryTable memoryTable(
            &keyComparer);

        StringKey key1;
        key1.set_value("key-1");
        StringValue value1;
        key1.set_value("value-1");

        MemoryTableRow row1
        {
            copy_unique(key1),
            static_cast<SequenceNumber>(5),
            copy_unique(value1),
            nullptr,
        };

        co_await memoryTable.AddRow(
            SequenceNumber::Earliest,
            row1);

        StringKey key2;
        key2.set_value("key-1");
        StringValue value2;
        key2.set_value("value-1");

        MemoryTableRow row2
        {
            copy_unique(key2),
            static_cast<SequenceNumber>(4),
            copy_unique(value2),
            nullptr,
        };

        ASSERT_THROW(
            co_await memoryTable.AddRow(
                SequenceNumber::Latest,
                row2),
            WriteConflict);
    });
}

TEST(MemoryTableTests, Add_new_version_of_row)
{
    run_async([]()->task<>
    {
        KeyComparer keyComparer(
            StringKey::descriptor());

        MemoryTable memoryTable(
            &keyComparer);

        StringKey key1;
        key1.set_value("key-1");
        StringValue value1;
        key1.set_value("value-1");

        MemoryTableRow row1
        {
            copy_unique(key1),
            static_cast<SequenceNumber>(5),
            copy_unique(value1),
            nullptr,
        };

        co_await memoryTable.AddRow(
            SequenceNumber::Earliest,
            row1);

        StringKey key2;
        key2.set_value("key-1");
        StringValue value2;
        key2.set_value("value-1");

        MemoryTableRow row2
        {
            copy_unique(key2),
            static_cast<SequenceNumber>(6),
            copy_unique(value2),
            nullptr,
        };

        co_await memoryTable.AddRow(
            SequenceNumber::Latest,
            row2);
    });
}}