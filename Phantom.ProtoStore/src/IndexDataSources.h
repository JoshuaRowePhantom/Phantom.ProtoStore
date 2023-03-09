#pragma once

#include "StandardTypes.h"
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"

namespace Phantom::ProtoStore
{

class IIndexDataSources : public SerializationTypes
{
public:
    virtual task<> Replay(
        FlatMessage<LoggedRowWrite> rowWrite
    ) = 0;

    virtual task<> Replay(
        const LoggedCheckpoint* loggedCheckpoint
    ) = 0;

    virtual task<> FinishReplay(
    ) = 0;

    virtual task<LoggedCheckpointT> StartCheckpoint(
    ) = 0;

    virtual task<WriteRowsResult> Checkpoint(
        const LoggedCheckpointT& loggedCheckpoint,
        shared_ptr<IPartitionWriter> partitionWriter
    ) = 0;

    virtual task<> UpdatePartitions(
        const LoggedCheckpointT& loggedCheckpoint,
        vector<shared_ptr<IPartition>> partitions
    ) = 0;
};
}
