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
};

ASYNC_TEST_F(IndexPartitionMergeGeneratorTests, Does_not_generate_merges_when_merges_per_level_is_not_exceeded)
{
    co_return;
}
}