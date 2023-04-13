#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/IndexPartitionMergeGenerator.h"
#include "TestFactories.h"

namespace Phantom::ProtoStore
{

class IndexPartitionMergeGeneratorTests :
    public testing::Test,
    public TestFactories
{
public:
    IndexPartitionMergeGenerator mergeGenerator;
    MergeParameters mergeParameters_merges_2_maxLevel_3;

    IndexPartitionMergeGeneratorTests()
    {
        mergeParameters_merges_2_maxLevel_3.set_mergesperlevel(2);
        mergeParameters_merges_2_maxLevel_3.set_maxlevel(3);
    }

    void DoTest(
        const json_row_list& partitions,
        const json_row_list& existingMerges,
        const json_row_list& expectedMergeCandidates
    )
    {
        auto partitionsRows = JsonToFlatRows<FlatBuffers::PartitionsKey, FlatBuffers::PartitionsValue>(
            partitions);

        auto existingMergesRows = JsonToFlatRows<FlatBuffers::MergesKey, FlatBuffers::MergesValue>(
            existingMerges
        );

        auto actualMergeCandidateRows = mergeGenerator.GetMergeCandidates(
            mergeParameters_merges_2_maxLevel_3,
            partitionsRows,
            existingMergesRows);

        auto expectedMergeCandidateRows = JsonToFlatRows<FlatBuffers::MergesKey, FlatBuffers::MergesValue>(
            expectedMergeCandidates
        );

        EXPECT_TRUE(RowListsAreEqual(
            expectedMergeCandidateRows,
            actualMergeCandidateRows)
        );
    }
};

TEST_F(IndexPartitionMergeGeneratorTests, Does_not_generate_merges_when_merges_per_level_is_not_exceeded)
{
    DoTest(
        {
            // Level 1
            {
                "{ index_number: 5, header_extent_name : { index_extent_name : { partition_number : 1, index_number : 5, level : 1 } } }",
                "{ latest_partition_number : 1 }",
            },
            // Level 2
            {
                "{ index_number: 5, header_extent_name : { index_extent_name : { partition_number : 2, index_number : 5, level : 2 } } }",
                "{ latest_partition_number : 1 }",
            },
        },
        {},
        {}
    );
}

TEST_F(IndexPartitionMergeGeneratorTests, Does_generate_merges_when_merges_per_level_is_not_exceeded)
{
    DoTest(
        {
            // Level 1
            {
                "{ index_number: 5, header_extent_name : { index_extent_name : { partition_number : 1, index_number : 5, level : 1 } } }",
                "{ latest_partition_number : 1 }",
            },
            {
                "{ index_number: 5, header_extent_name : { index_extent_name : { partition_number : 2, index_number : 5, level : 1 } } }",
                "{ latest_partition_number : 4 }",
            },
            // Level 2
            {
                "{ index_number: 5, header_extent_name : { index_extent_name : { partition_number : 3, index_number : 5, level : 2 } } }",
                "{ latest_partition_number : 1 }",
            },
        },
        {},
        {
            {
                "{ index_number: 5, merges_unique_id : { index_extent_name : { partition_number : 1, index_number : 5, level : 1 } } }",
                "{ source_header_extent_names : ["
                    "{ index_extent_name : { partition_number : 1, index_number : 5, level : 1 } }, "
                    "{ index_extent_name : { partition_number : 2, index_number : 5, level : 1 } }"
                "], source_level_number : 1, destination_level_number : 2, latest_partition_number : 4 }"
            }
        }
    );
}

TEST_F(IndexPartitionMergeGeneratorTests, Generated_merges_are_at_max_level)
{
    DoTest(
        {
            // Level 4
            {
                "{ index_number: 5, header_extent_name : { index_extent_name : { partition_number : 1, index_number : 5, level : 4 } } }",
                "{ latest_partition_number : 1 }",
            },
            {
                "{ index_number: 5, header_extent_name : { index_extent_name : { partition_number : 2, index_number : 5, level : 4 } } }",
                "{ latest_partition_number : 4 }",
            },
            // Level 2
            {
                "{ index_number: 5, header_extent_name : { index_extent_name : { partition_number : 3, index_number : 5, level : 2 } } }",
                "{ latest_partition_number : 1 }",
            },
        },
        {},
        {
            {
                "{ index_number: 5, merges_unique_id : { index_extent_name : { partition_number : 1, index_number : 5, level : 4 } } }",
                "{ source_header_extent_names : ["
                    "{ index_extent_name : { partition_number : 1, index_number : 5, level : 1 } }, "
                    "{ index_extent_name : { partition_number : 2, index_number : 5, level : 1 } }"
                "], source_level_number : 3, destination_level_number : 3, latest_partition_number : 4 }"
            }
        }
        );
}

TEST_F(IndexPartitionMergeGeneratorTests, Does_not_generate_merges_for_partitions_already_being_merged)
{
    DoTest(
        {
            // Level 1
            {
                "{ index_number: 5, header_extent_name : { index_extent_name : { partition_number : 1, index_number : 5, level : 1 } } }",
                "{ latest_partition_number : 1 }",
            },
            {
                "{ index_number: 5, header_extent_name : { index_extent_name : { partition_number : 2, index_number : 5, level : 1 } } }",
                "{ latest_partition_number : 4 }",
            },
            {
                "{ index_number: 5, header_extent_name : { index_extent_name : { partition_number : 3, index_number : 5, level : 1 } } }",
                "{ latest_partition_number : 4 }",
            },
            // Level 2
            {
                "{ index_number: 5, header_extent_name : { index_extent_name : { partition_number : 4, index_number : 5, level : 2 } } }",
                "{ latest_partition_number : 1 }",
            },
        },
        {
            {
                "{ index_number: 5, merges_unique_id : { index_extent_name : { partition_number : 1, index_number : 5, level : 1 } } }",
                "{ source_header_extent_names : ["
                    "{ index_extent_name : { partition_number : 1, index_number : 5, level : 1 } }, "
                    "{ index_extent_name : { partition_number : 2, index_number : 5, level : 1 } }"
                "], source_level_number : 1, destination_level_number : 2, latest_partition_number : 4 }"
            }
        },
        {}
        );
}

TEST_F(IndexPartitionMergeGeneratorTests, Does_generate_merges_for_partitions_not_already_being_merged)
{
    DoTest(
        {
            // Level 1
            {
                "{ index_number: 5, header_extent_name : { index_extent_name : { partition_number : 1, index_number : 5, level : 1 } } }",
                "{ latest_partition_number : 1 }",
            },
            {
                "{ index_number: 5, header_extent_name : { index_extent_name : { partition_number : 2, index_number : 5, level : 1 } } }",
                "{ latest_partition_number : 5 }",
            },
            {
                "{ index_number: 5, header_extent_name : { index_extent_name : { partition_number : 3, index_number : 5, level : 1 } } }",
                "{ latest_partition_number : 7 }",
            },
            {
                "{ index_number: 5, header_extent_name : { index_extent_name : { partition_number : 4, index_number : 5, level : 1 } } }",
                "{ latest_partition_number : 4 }",
            },
            // Level 2
            {
                "{ index_number: 5, header_extent_name : { index_extent_name : { partition_number : 5, index_number : 5, level : 2 } } }",
                "{ latest_partition_number : 1 }",
            },
        },
        {
            {
                "{ index_number: 5, merges_unique_id : { index_extent_name : { partition_number : 1, index_number : 5, level : 1 } } }",
                "{ source_header_extent_names : ["
                    "{ index_extent_name : { partition_number : 1, index_number : 5, level : 1 } }, "
                    "{ index_extent_name : { partition_number : 2, index_number : 5, level : 1 } }"
                "], source_level_number : 1, destination_level_number : 2, latest_partition_number : 4 }"
            }
        },
        {
            {
                "{ index_number: 5, merges_unique_id : { index_extent_name : { partition_number : 3, index_number : 5, level : 1 } } }",
                "{ source_header_extent_names : ["
                    "{ index_extent_name : { partition_number : 3, index_number : 5, level : 1 } }, "
                    "{ index_extent_name : { partition_number : 4, index_number : 5, level : 1 } }"
                "], source_level_number : 1, destination_level_number : 2, latest_partition_number : 7 }"
            }
        }
    );
}

}