#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/ExistingPartitions.h"
#include "Phantom.ProtoStore/src/Resources.h"
#include "TestFactories.h"

namespace Phantom::ProtoStore
{

class ExistingPartitionsTests : public testing::Test
{
public:
    std::shared_ptr<IIndexData> partitionsIndex;
    std::shared_ptr<ExistingPartitions> existingPartitions;

    task<> AsyncSetUp()
    {
        partitionsIndex = co_await MakeInMemoryIndex(
            "Partitions",
            Schema::Make(
                { FlatBuffersSchemas::ProtoStoreInternalSchema, FlatBuffersSchemas::PartitionsKey_Object },
                { FlatBuffersSchemas::ProtoStoreInternalSchema, FlatBuffersSchemas::PartitionsValue_Object }
        ));

        existingPartitions = Phantom::ProtoStore::MakeExistingPartitions(
            partitionsIndex);
    }
};

ASYNC_TEST_F(ExistingPartitionsTests, BeginReplay_adds_preexisting_partitions)
{
    co_return;
}


}