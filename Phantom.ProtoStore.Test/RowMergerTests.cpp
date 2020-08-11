#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/RowMerger.h"
#include "Phantom.ProtoStore/src/KeyComparer.h"
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
        row_generator (RowMerger::*mergeFunction)(row_generators)
    )
    {
        run_async([&]() -> task<>
        {
            KeyComparer keyComparer(
                StringKey::descriptor());

            RowMerger rowMerger(
                &keyComparer);

            auto convertTestRowsToRowGenerator = [](test_row_list_type& sourceRows) -> row_generator
            {
                StringKey stringKey;
                StringValue stringValue;

                for (auto& testRow : sourceRows)
                {
                    stringKey.set_value(get<0>(testRow));

                    if (get<1>(testRow))
                    {
                        stringValue.set_value(*get<1>(testRow));
                    }

                    ResultRow resultRow =
                    {
                        .Key = &stringKey,
                        .WriteSequenceNumber = ToSequenceNumber(get<2>(testRow)),
                        .Value = get<1>(testRow) ? &stringValue : nullptr,
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

            auto rowMergerEnumeration = (rowMerger.*mergeFunction)(
                rowSources());

            test_row_list_type actualMergedTestRows;

            for (auto rowMergerIterator = co_await rowMergerEnumeration.begin();
                rowMergerIterator != rowMergerEnumeration.end();
                co_await ++rowMergerIterator)
            {
                auto key = static_cast<const StringKey*>((*rowMergerIterator).Key);
                auto value = static_cast<const StringValue*>((*rowMergerIterator).Value);
                auto writeSequenceNumber = (*rowMergerIterator).WriteSequenceNumber;

                auto testRow = std::make_tuple(
                    key->value(),
                    !value ? optional<string>() : value->value(),
                    ToUint64(writeSequenceNumber));

                actualMergedTestRows.push_back(testRow);
            }

            ASSERT_EQ(
                actualMergedTestRows,
                expectedMergedTestRows);
        });
    }

    void DoRowMergerTestPermutations(
        test_row_list_list_type sourceTestRows,
        test_row_list_type expectedMergedTestRows,
        row_generator(RowMerger::* mergeFunction)(row_generators)
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

}