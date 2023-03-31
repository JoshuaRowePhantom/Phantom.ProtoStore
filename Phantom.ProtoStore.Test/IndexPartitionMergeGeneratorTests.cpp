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
};

ASYNC_TEST_F(IndexPartitionMergeGeneratorTests, Does_not_generate_merges_when_merges_per_level_is_not_exceeded)
{
    auto partitionsRows = JsonToFlatRows<FlatBuffers::PartitionsKey, FlatBuffers::PartitionsValue>(
        {
            {
                "{ index_number: 5, header_extent_name : { index_extent_name : { partition_number : 1, index_number : 5, level : 1 } } }",
                "{ data_extent_name : { index_extent_name : { partition_number : 1, index_number : 5, level : 1 } }, level : 1, latest_checkpoint_number : 1 }",
            }
        });

    auto existingMerges = JsonToFlatRows<FlatBuffers::MergesKey, FlatBuffers::MergesValue>(
        {}
    );

    auto actualMergeCandidateRows = mergeGenerator.GetMergeCandidates(
        mergeParameters_merges_2_maxLevel_3,
        partitionsRows,
        existingMerges);

    auto expectedMergeCandidateRows = JsonToFlatRows<FlatBuffers::MergesKey, FlatBuffers::MergesValue>(
        {}
    );

    EXPECT_TRUE(RowListsAreEqual(
        expectedMergeCandidateRows,
        actualMergeCandidateRows)
    );

    co_return;
}

}