#pragma once

#include "StandardTypes.h"

namespace Phantom::ProtoStore
{

class IIndexDataSources
{
public:

    virtual task<> Replay(
        const LoggedRowWrite& rowWrite
    ) = 0;

    virtual task<> Replay(
        const LoggedCheckpoint& loggedCheckpoint
    ) = 0;

    virtual task<> FinishReplay(
    ) = 0;

    virtual task<LoggedCheckpoint> StartCheckpoint(
    ) = 0;

    virtual task<> Checkpoint(
        const LoggedCheckpoint& loggedCheckpoint,
        shared_ptr<IPartitionWriter> partitionWriter
    ) = 0;

    virtual task<> UpdatePartitions(
        const LoggedCheckpoint& loggedCheckpoint,
        vector<shared_ptr<IPartition>> partitions
    ) = 0;
};
}
