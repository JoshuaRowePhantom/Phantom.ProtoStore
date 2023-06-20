#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/ExtentName.h"
#include "Phantom.ProtoStore/src/ExistingPartitions.h"
#include "Phantom.ProtoStore/src/Resources.h"
#include "TestFactories.h"

namespace Phantom::ProtoStore
{

class ExistingPartitionsTests : 
    public testing::Test,
    public TestFactories
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
        key.index_number = indexNumber;
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

    task<> RemovePartitionsRow(
        IndexNumber indexNumber,
        PartitionNumber partitionNumber
    )
    {
        FlatBuffers::PartitionsKeyT key;
        key.index_number = indexNumber;
        key.header_extent_name = copy_unique(*MakePartitionHeaderExtentName(
            indexNumber,
            partitionNumber,
            1,
            "index"
        ).extent_name.AsIndexHeaderExtentName());

        auto result = co_await AddRow(
            partitionsIndex,
            &key,
            nullptr);

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


ASYNC_TEST_F(ExistingPartitionsTests, Replay_LoggedCreatePartition_adds_partition_to_existing_partitions)
{
    EXPECT_EQ(false, co_await existingPartitions->DoesPartitionNumberExist(1008));

    FlatBuffers::LoggedCreatePartitionT loggedCreatePartitionT;
    auto extentName = MakePartitionHeaderExtentName(
        1010,
        1008,
        1,
        "");
    loggedCreatePartitionT.header_extent_name = copy_unique(*extentName.extent_name.AsIndexHeaderExtentName());

    FlatMessage loggedCreatePartition{ &loggedCreatePartitionT };
    co_await existingPartitions->Replay(
        loggedCreatePartition);

    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(1008));
}

ASYNC_TEST_F(ExistingPartitionsTests, Replay_LoggedRowWrite_adds_partition_to_existing_partitions)
{
    EXPECT_EQ(false, co_await existingPartitions->DoesPartitionNumberExist(1008));

    FlatBuffers::LoggedRowWriteT loggedRowWriteT;
    loggedRowWriteT.index_number = 1010;
    loggedRowWriteT.partition_number = 1008;

    FlatMessage loggedRowWrite{ &loggedRowWriteT };
    co_await existingPartitions->Replay(
        loggedRowWrite);

    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(1008));
}

ASYNC_TEST_F(ExistingPartitionsTests, Replay_LoggedUpdatePartitions_replaces_content_for_that_index_with_values_from_Partitions_Index)
{
    co_await AddPartitionsRow(
        1002,
        100000);

    co_await existingPartitions->BeginReplay();

    {
        FlatBuffers::LoggedCreatePartitionT loggedCreatePartition;
        auto extentName = MakePartitionHeaderExtentName(
            1001,
            100001,
            1,
            "");
        loggedCreatePartition.header_extent_name = copy_unique(*extentName.extent_name.AsIndexHeaderExtentName());

        co_await existingPartitions->Replay(
            FlatMessage{ &loggedCreatePartition });
    }

    {
        FlatBuffers::LoggedCreatePartitionT loggedCreatePartition;
        auto extentName = MakePartitionHeaderExtentName(
            1002,
            100002,
            1,
            "");
        loggedCreatePartition.header_extent_name = copy_unique(*extentName.extent_name.AsIndexHeaderExtentName());

        co_await existingPartitions->Replay(
            FlatMessage{ &loggedCreatePartition });
    }

    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(100000));
    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(100001));
    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(100002));
    EXPECT_EQ(false, co_await existingPartitions->DoesPartitionNumberExist(100003));

    co_await AddPartitionsRow(
        1002,
        100003);

    co_await RemovePartitionsRow(
        1002,
        100000);

    {
        FlatBuffers::LoggedUpdatePartitionsT loggedUpdatePartitions;
        loggedUpdatePartitions.index_number = 1002;

        co_await existingPartitions->Replay(
            FlatMessage{ &loggedUpdatePartitions });
    }

    EXPECT_EQ(false, co_await existingPartitions->DoesPartitionNumberExist(100000));
    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(100001));
    EXPECT_EQ(false, co_await existingPartitions->DoesPartitionNumberExist(100002));
    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(100003));
}

ASYNC_TEST_F(ExistingPartitionsTests, FinishReplay_removes_uncommitted_LoggedCreatePartition_partitions)
{
    co_await AddPartitionsRow(
        1002,
        100000);

    co_await existingPartitions->BeginReplay();

    {
        FlatBuffers::LoggedCreatePartitionT loggedCreatePartition;
        auto extentName = MakePartitionHeaderExtentName(
            1001,
            100001,
            1,
            "");
        loggedCreatePartition.header_extent_name = copy_unique(*extentName.extent_name.AsIndexHeaderExtentName());

        co_await existingPartitions->Replay(
            FlatMessage{ &loggedCreatePartition });
    }

    {
        FlatBuffers::LoggedCreatePartitionT loggedCreatePartition;
        auto extentName = MakePartitionHeaderExtentName(
            1002,
            100002,
            1,
            "");
        loggedCreatePartition.header_extent_name = copy_unique(*extentName.extent_name.AsIndexHeaderExtentName());

        co_await existingPartitions->Replay(
            FlatMessage{ &loggedCreatePartition });
    }

    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(100000));
    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(100001));
    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(100002));

    co_await existingPartitions->FinishReplay();

    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(100000));
    EXPECT_EQ(false, co_await existingPartitions->DoesPartitionNumberExist(100001));
    EXPECT_EQ(false, co_await existingPartitions->DoesPartitionNumberExist(100002));
}

ASYNC_TEST_F(ExistingPartitionsTests, FinishReplay_leaves_committed_partitions)
{
    co_await existingPartitions->BeginReplay();

    {
        FlatBuffers::LoggedCreatePartitionT loggedCreatePartition;
        auto extentName = MakePartitionHeaderExtentName(
            1001,
            100000,
            1,
            "");
        loggedCreatePartition.header_extent_name = copy_unique(*extentName.extent_name.AsIndexHeaderExtentName());

        co_await existingPartitions->Replay(
            FlatMessage{ &loggedCreatePartition });
    }

    co_await AddPartitionsRow(
        1001,
        100000);

    {
        FlatBuffers::LoggedUpdatePartitionsT loggedUpdatePartitions;
        loggedUpdatePartitions.index_number = 1001;

        co_await existingPartitions->Replay(
            FlatMessage{ &loggedUpdatePartitions });
    }

    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(100000));

    co_await existingPartitions->FinishReplay();

    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(100000));
}

}