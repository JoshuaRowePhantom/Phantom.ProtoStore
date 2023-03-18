#pragma once

#include "StandardTypes.h"

namespace Phantom::ProtoStore
{

class ExistingPartitions
{
public:
    virtual task<> BeginReplay(
    ) = 0;

    virtual task<> Replay(
        const FlatMessage<FlatBuffers::LoggedCreatePartition>& loggedCreatePartition
    ) = 0;

    virtual task<> Replay(
        const FlatMessage<FlatBuffers::LoggedUpdatePartitions>& loggedUpdatePartitions
    ) = 0;

    virtual task<> FinishReplay(
    ) = 0;

    virtual task<bool> DoesPartitionNumberExist(
        PartitionNumber
    ) = 0;
};

std::shared_ptr<ExistingPartitions> MakeExistingPartitions(
    std::shared_ptr<IIndexData> partitionsIndex
);

}