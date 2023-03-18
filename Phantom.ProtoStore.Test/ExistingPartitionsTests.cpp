#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/ExtentName.h"
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

    task<> AddPartitionsRow(
        IndexNumber indexNumber,
        PartitionNumber partitionNumber
    )
    {
        FlatBuffers::PartitionsKeyT key;
        key.index_number = 5;
        key.header_extent_name = copy_unique(*MakePartitionHeaderExtentName(
            indexNumber,
            partitionNumber,
            1,
            "index"
        ).extent_name.AsIndexHeaderExtentName());

        FlatBuffers::PartitionsValueT value;

        auto result = co_await AddRow(
            partitionsIndex,
            &key,
            &value);

        EXPECT_EQ(true, result.has_value());
    }
};

ASYNC_TEST_F(ExistingPartitionsTests, BeginReplay_adds_preexisting_partitions)
{
    co_await AddPartitionsRow(
        5,
        1005);

    co_await AddPartitionsRow(
        6,
        1006);

    co_await AddPartitionsRow(
        6,
        1007);

    co_await existingPartitions->BeginReplay();

    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(1005));
    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(1006));
    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(1007));
    EXPECT_EQ(false, co_await existingPartitions->DoesPartitionNumberExist(1008));
}


}