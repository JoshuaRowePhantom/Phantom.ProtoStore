#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/ProtocolBuffersValueComparer.h"
#include "Phantom.ProtoStore/src/RowMerger.h"
#include "ProtoStoreTest.pb.h"
#include <string>
#include <tuple>
#include <vector>

namespace Phantom::ProtoStore
{

class RowMergerTests : 
    public testing::Test
{
protected:
    typedef std::tuple<string, optional<string>, uint64_t> test_row_type;
    typedef std::vector<test_row_type> test_row_list_type;
    typedef std::vector<test_row_list_type> test_row_list_list_type;

    typedef std::optional<string> nil;

    void DoRowMergerTest(
        test_row_list_list_type sourceTestRows,
        test_row_list_type expectedMergedTestRows,
        std::invocable<RowMerger&, row_generators> auto mergeFunction
    )
    {
        run_async([&]() -> task<>
        {
            RowMerger rowMerger(
                std::make_shared<ProtocolBuffersValueComparer>(
                    StringKey::descriptor()));

            auto convertTestRowsToRowGenerator = [](test_row_list_type& sourceRows) -> row_generator
            {
                StringKey stringKey;
                StringValue stringValue;

                for (auto& testRow : sourceRows)
                {
                    stringKey.set_value(get<0>(testRow));

                    ProtoValue stringValueProto;

                    if (get<1>(testRow))
                    {
                        stringValue.set_value(*get<1>(testRow));
                        stringValueProto = ProtoValue{ &stringValue }.pack();
                    }

                    ResultRow resultRow =
                    {
                        .Key = ProtoValue(&stringKey).pack(),
                        .WriteSequenceNumber = ToSequenceNumber(get<2>(testRow)),
                        .Value = std::move(stringValueProto),
                    };

                    co_yield resultRow;
                }
            };

            auto rowSources = [&]() -> row_generators
            {
                for (auto& sourceTestRowList : sourceTestRows)
                {
                    co_yield convertTestRowsToRowGenerator(
                        sourceTestRowList);
                }
            };

            auto rowMergerEnumeration = std::invoke(
                mergeFunction,
                rowMerger,
                rowSources());

            test_row_list_type actualMergedTestRows;

            for (auto rowMergerIterator = co_await rowMergerEnumeration.begin();
                rowMergerIterator != rowMergerEnumeration.end();
                co_await ++rowMergerIterator)
            {
                StringKey key;
                StringValue value;
                rowMergerIterator->Key.unpack(&key);
                rowMergerIterator->Value.unpack(&value);

                auto writeSequenceNumber = (*rowMergerIterator).WriteSequenceNumber;

                auto testRow = std::make_tuple(
                    key.value(),
                    !rowMergerIterator->Value ? optional<string>() : value.value(),
                    ToUint64(writeSequenceNumber));

                actualMergedTestRows.push_back(testRow);
            }

            EXPECT_EQ(
                actualMergedTestRows,
                expectedMergedTestRows);
        });
    }

    void DoRowMergerTestPermutations(
        test_row_list_list_type sourceTestRows,
        test_row_list_type expectedMergedTestRows,
        std::invocable<RowMerger&, row_generators> auto mergeFunction
    )
    {
        DoRowMergerTest(
            sourceTestRows,
            expectedMergedTestRows,
            mergeFunction
        );

        std::reverse(
            sourceTestRows.begin(),
            sourceTestRows.end()
        );

        DoRowMergerTest(
            sourceTestRows,
            expectedMergedTestRows,
            mergeFunction);
    }
};

TEST_F(RowMergerTests, Can_merge_empty)
{
    DoRowMergerTest(
        {},
        {},
        &RowMerger::Merge
    );
}

TEST_F(RowMergerTests, Can_merge_one_empty)
{
    DoRowMergerTest(
        {
            {},
        },
        {},
        & RowMerger::Merge
    );
}

TEST_F(RowMergerTests, Can_merge_two_empty)
{
    DoRowMergerTest(
        {
            {},
            {},
        },
        {},
        & RowMerger::Merge
    );
}

TEST_F(RowMergerTests, Can_merge_one_non_empty)
{
    DoRowMergerTestPermutations(
        {
            {
                {"a","a-v",2},
            },
        },
        {
            {"a","a-v",2},
        },
        & RowMerger::Merge
    );
}

TEST_F(RowMergerTests, Can_merge_two_sources_different_keys)
{
    DoRowMergerTestPermutations(
        {
            {
                {"a","a-v",2},
            },
            {
                {"b","b-v",2},
            },
        },
        {
            {"a","a-v",2},
            {"b","b-v",2},
        },
        & RowMerger::Merge
        );
}

TEST_F(RowMergerTests, Can_merge_two_sources_same_key_in_reverse_write_sequence_number_order)
{
    DoRowMergerTestPermutations(
        {
            {
                {"a","a-v1",1},
            },
            {
                {"a","a-v2",2},
            },
        },
        {
            {"a","a-v2",2},
            {"a","a-v1",1},
        },
        & RowMerger::Merge
        );
}

TEST_F(RowMergerTests, Can_merge_three_sources_one_shorter)
{
    DoRowMergerTestPermutations(
        {
            {
                {"a","a-v1",1},
                {"b","b-v2",2},
            },
            {
                {"a","a-v2",2},
                {"b","b-v1",1},
                {"c","c-v1",1},
            },
            {
                {"c","c-v2",2},
            }
        },
        {
            {"a","a-v2",2},
            {"a","a-v1",1},
            {"b","b-v2",2},
            {"b","b-v1",1},
            {"c","c-v2",2},
            {"c","c-v1",1},
        },
        & RowMerger::Merge
        );
}

TEST_F(RowMergerTests, Can_enumerate_three_sources_with_deletes)
{
    DoRowMergerTestPermutations(
        {
            {
                {"a","a-v1",1},
                {"b","b-v2",2},
                {"d",nil(),2},
                {"e","e-v3",3},
                {"e","e-v2",2},
            },
            {
                {"a","a-v2",2},
                {"b","b-v1",1},
                {"c","c-v1",1},
            },
            {
                {"c","c-v2",2},
                {"d","d-v1",1},
                {"e",nil(),1},
            }
        },
        {
            {"a","a-v2",2},
            {"b","b-v2",2},
            {"c","c-v2",2},
            {"e","e-v3",3},
        },
        & RowMerger::Enumerate
        );
}

TEST_F(RowMergerTests, FilterTopLevelMergeSnapshotWindowRows_keeps_rows_up_to_first_below_earliest_snapshot_window_except_deleted_rows)
{
    DoRowMergerTestPermutations(
        {
            {
                {"a", "5", 5},
                {"a", "4", 4},
                {"a", "3", 3},
                {"a", "2", 2},
                {"a", "1", 1},
            },
            {
                {"b", "5", 5},
                {"b", "4", 4},
                {"b", nil(), 3},
                {"b", "2", 2},
                {"b", "1", 1},
            },
            {
                {"c", "5", 5},
                {"c", "4", 4},
                {"c", "3", 3},
                {"c", nil(), 2},
                {"c", "1", 1},
            },
            {
                {"d", "5", 5},
                {"d", "4", 4},
                {"d", "3", 3},
                {"d", "2", 2},
                {"d", nil(), 1},
            },
            {
                {"e", "5", 5},
                {"e", "4", 4},
            },
            {
                {"f", "2", 2},
                {"f", nil(), 1},
            },
            {
                // The delete should be kept, because it is = the snapshot window
                {"g", nil(), 3},
                {"g", "2", 2},
                {"g", nil(), 1},
            },
            {
                {"h", "3", 3},
                {"h", "2", 2},
                {"h", nil(), 1},
            },
            {
                {"i", "5", 5},
                {"i", nil(), 1},
            },
            {
                {"j", "5", 5},
                {"j", "1", 1},
            },
            {
                {"k", nil(), 5},
                {"k", "3", 3},
                {"k", "2", 2},
            },
            {
                {"l", nil(), 5},
                {"l", "2", 2},
            },
            {
                {"m", nil(), 4},
                // The delete should be kept, because it is = the snapshot window
                {"m", nil(), 3},
                {"m", nil(), 2},
            },
            {
                {"n", "4", 4},
                {"n", nil(), 3},
                {"n", nil(), 2},
            },
            {
                {"o", "4", 4},
                // The delete should be dropped, because it is < the snapshot window.
                {"o", nil(), 2},
                {"o", "1", 1},
            },
        },
        {
            {"a", "5", 5},
            {"a", "4", 4},
            {"a", "3", 3},
            {"b", "5", 5},
            {"b", "4", 4},
            {"b", nil(), 3},
            {"c", "5", 5},
            {"c", "4", 4},
            {"c", "3", 3},
            {"d", "5", 5},
            {"d", "4", 4},
            {"d", "3", 3},
            {"e", "5", 5},
            {"e", "4", 4},
            {"f", "2", 2},
            {"g", nil(), 3},
            {"h", "3", 3},
            {"i", "5", 5},
            {"j", "5", 5},
            {"j", "1", 1},
            {"k", nil(), 5},
            {"k", "3", 3},
            {"l", nil(), 5},
            {"l", "2", 2},
            {"m", nil(), 4},
            {"m", nil(), 3},
            {"n", "4", 4},
            {"n", nil(), 3},
            {"o", "4", 4},
        },
        [](RowMerger& rowMerger, auto rowSources) -> row_generator
    {
        return rowMerger.FilterTopLevelMergeSnapshotWindowRows(
            rowMerger.Merge(std::move(rowSources)),
            ToSequenceNumber(3));
    }
    );
}

}