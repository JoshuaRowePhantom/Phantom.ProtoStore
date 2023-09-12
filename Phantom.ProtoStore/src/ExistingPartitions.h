#pragma once

#include "StandardTypes.h"
#include "LogReplayTarget.h"

namespace Phantom::ProtoStore
{

class ExistingPartitions :
    public LogReplayTarget
{
public:
    virtual task<bool> DoesPartitionNumberExist(
        PartitionNumber
    ) = 0;
};

std::shared_ptr<ExistingPartitions> MakeExistingPartitions(
    std::shared_ptr<IIndexData> partitionsIndex
);

}