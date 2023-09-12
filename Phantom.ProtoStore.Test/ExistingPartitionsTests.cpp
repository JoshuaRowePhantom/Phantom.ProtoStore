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

    task<> LogRowWrite(
        IndexNumber indexNumber,
        PartitionNumber partitionNumber
    )
    {
        FlatBuffers::LoggedRowWriteT loggedRowWriteT;
        loggedRowWriteT.index_number = indexNumber;
        loggedRowWriteT.partition_number = partitionNumber;

        FlatMessage loggedRowWrite{ &loggedRowWriteT };
        co_await existingPartitions->Replay(
            loggedRowWrite);
    }

    task<> LogCheckpoint(
        IndexNumber indexNumber,
        std::vector<PartitionNumber> partitionNumbers
    )
    {
        FlatBuffers::LoggedCheckpointT loggedCheckpointT;
        loggedCheckpointT.index_number = indexNumber;
        loggedCheckpointT.partition_number = partitionNumbers;

        FlatMessage loggedCheckpoint{ &loggedCheckpointT };
        co_await existingPartitions->Replay(
            loggedCheckpoint);
    }

    task<> LogPartitionsData(
    )
    {
        FlatBuffers::LoggedPartitionsDataT loggedPartitionsData;

        co_await existingPartitions->Replay(
            FlatMessage{ &loggedPartitionsData });
    }

    task<> LogCreateIndexHeaderExtent(
        IndexNumber indexNumber,
        PartitionNumber partitionNumber
    )
    {
        FlatBuffers::LoggedCreateExtentT loggedCreateExtent;
        auto extentName = MakePartitionHeaderExtentName(
            indexNumber,
            partitionNumber,
            1,
            "");
        loggedCreateExtent.extent_name = copy_unique(extentName);

        co_await existingPartitions->Replay(
            FlatMessage { &loggedCreateExtent });
    }
    
    task<> LogDeleteIndexHeaderExtent(
        IndexNumber indexNumber,
        PartitionNumber partitionNumber
    )
    {
        FlatBuffers::LoggedDeleteExtentT loggedDeleteExtent;
        auto extentName = MakePartitionHeaderExtentName(
            indexNumber,
            partitionNumber,
            1,
            "");
        loggedDeleteExtent.extent_name = copy_unique(extentName);

        co_await existingPartitions->Replay(
            FlatMessage { &loggedDeleteExtent });
    }

    task<> LogCommitIndexHeaderExtent(
        IndexNumber indexNumber,
        PartitionNumber partitionNumber
    )
    {
        FlatBuffers::LoggedCommitExtentT loggedCommitExtent;
        auto extentName = MakePartitionHeaderExtentName(
            indexNumber,
            partitionNumber,
            1,
            "");
        loggedCommitExtent.extent_name = copy_unique(extentName);

        co_await existingPartitions->Replay(
            FlatMessage { &loggedCommitExtent });
    }
};

ASYNC_TEST_F(ExistingPartitionsTests, Replay_LoggedCreatePartition_adds_partition_to_existing_partitions)
{
    EXPECT_EQ(false, co_await existingPartitions->DoesPartitionNumberExist(1008));

    co_await LogCreateIndexHeaderExtent(1010, 1008);

    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(1008));
}

ASYNC_TEST_F(ExistingPartitionsTests, Replay_LoggedDeletePartition_remove_partition)
{
    EXPECT_EQ(false, co_await existingPartitions->DoesPartitionNumberExist(1008));

    co_await LogCreateIndexHeaderExtent(1010, 1008);

    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(1008));

    co_await LogDeleteIndexHeaderExtent(1010, 1008);

    EXPECT_EQ(false, co_await existingPartitions->DoesPartitionNumberExist(1008));
}

ASYNC_TEST_F(ExistingPartitionsTests, Replay_LoggedRowWrite_adds_partition_to_existing_partitions)
{
    EXPECT_EQ(false, co_await existingPartitions->DoesPartitionNumberExist(1008));

    co_await LogRowWrite(1010, 1008);

    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(1008));
}

ASYNC_TEST_F(ExistingPartitionsTests, Replay_LoggedCheckpoint_removes_existing_LoggedRowWrite_partition)
{
    EXPECT_EQ(false, co_await existingPartitions->DoesPartitionNumberExist(1008));

    co_await LogRowWrite(1010, 1008);

    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(1008));

    co_await LogCheckpoint(1010, { 1008 });

    EXPECT_EQ(false, co_await existingPartitions->DoesPartitionNumberExist(1008));
}

ASYNC_TEST_F(ExistingPartitionsTests, Replay_LoggedPartitionsData_adds_existing_partitions)
{
    co_await existingPartitions->BeginReplay();

    co_await LogRowWrite(1010, 1008);

    co_await AddPartitionsRow(
        1010,
        100000);

    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(1008));
    EXPECT_EQ(false, co_await existingPartitions->DoesPartitionNumberExist(100000));

    co_await LogPartitionsData();

    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(1008));
    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(100000));
}

ASYNC_TEST_F(ExistingPartitionsTests, FinishReplay_removes_uncommitted_LoggedCreateExtent_partitions)
{
    co_await existingPartitions->BeginReplay();

    co_await LogCreateIndexHeaderExtent(1001, 100001);

    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(100001));

    co_await existingPartitions->FinishReplay();

    EXPECT_EQ(false, co_await existingPartitions->DoesPartitionNumberExist(100001));
}

ASYNC_TEST_F(ExistingPartitionsTests, FinishReplay_leaves_LoggedCommittedExtent_partitions)
{
    co_await existingPartitions->BeginReplay();

    co_await LogCreateIndexHeaderExtent(1001, 100000);
    co_await LogCommitIndexHeaderExtent(1001, 100000);

    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(100000));

    co_await existingPartitions->FinishReplay();

    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(100000));
}

ASYNC_TEST_F(ExistingPartitionsTests, FinishReplay_leaves_LoggedRowWrite_partitions)
{
    co_await existingPartitions->BeginReplay();

    co_await LogRowWrite(1010, 1008);
    
    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(1008));

    co_await existingPartitions->FinishReplay();

    EXPECT_EQ(true, co_await existingPartitions->DoesPartitionNumberExist(1008));
}

}