#include "StandardIncludes.h"
#include "TestFactories.h"

namespace Phantom::ProtoStore
{
class IndexTests :
    public testing::Test,
    public TestFactories
{
public:
};

ASYNC_TEST_F(IndexTests, can_insert_row_when_no_rows_are_present)
{
    auto testInMemoryIndex = co_await CreateTestInMemoryIndex(
        {}
    );

    throw_if_failed(co_await AddTestRow(
        testInMemoryIndex.Index,
        ToSequenceNumber(1),
        {
            "key",
            "value",
        }));
}

ASYNC_TEST_F(IndexTests, cannot_insert_row_when_it_conflicts_with_row_by_read_sequence_number_in_partition)
{
    auto testInMemoryIndex = co_await CreateTestInMemoryIndex(
        TestInMemoryIndexData
        {
            .Partitions =
            {
                {
                    .Rows =
                    {
                        TestStringKeyValuePairRow
                        {
                            .Key = "key",
                            .Value = "value",
                            .WriteSequenceNumber = ToSequenceNumber(10),
                        }
                    }
                }
            },
        }
    );

    auto result = co_await AddTestRow(
        testInMemoryIndex.Index,
        ToSequenceNumber(1),
        {
            .Key = "key",
            .Value = "value",
            .WriteSequenceNumber = ToSequenceNumber(12),
        });

    EXPECT_EQ(
        (std::unexpected{ FailedResult
        {
            .ErrorCode = make_error_code(ProtoStoreErrorCode::WriteConflict),
            .ErrorDetails = WriteConflict
            {
                .ConflictingSequenceNumber = ToSequenceNumber(10),
            }
        } }),
        result);
}

ASYNC_TEST_F(IndexTests, cannot_insert_row_when_it_conflicts_with_row_by_read_sequence_number_in_inactive_memory_table)
{
    auto testInMemoryIndex = co_await CreateTestInMemoryIndex(
        TestInMemoryIndexData
        {
            .InactiveMemoryTables =
            {
                {
                    .Rows =
                    {
                        TestStringKeyValuePairRow
                        {
                            .Key = "key",
                            .Value = "value",
                            .WriteSequenceNumber = ToSequenceNumber(10),
                        }
                    }
                }
            },
        }
    );

    auto result = co_await AddTestRow(
        testInMemoryIndex.Index,
        ToSequenceNumber(1),
        {
            .Key = "key",
            .Value = "value",
            .WriteSequenceNumber = ToSequenceNumber(12),
        });

    EXPECT_EQ(
        (std::unexpected{ FailedResult
        {
            .ErrorCode = make_error_code(ProtoStoreErrorCode::WriteConflict),
            .ErrorDetails = WriteConflict
            {
                .ConflictingSequenceNumber = ToSequenceNumber(10),
            }
        } }),
        result);
}


}