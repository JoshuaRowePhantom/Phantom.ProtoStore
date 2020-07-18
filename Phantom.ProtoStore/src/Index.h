#pragma once

#include "StandardTypes.h"

namespace Phantom::ProtoStore
{

typedef google::protobuf::uint64 CheckpointNumber;

class IIndex
{
public:
    virtual task<CheckpointNumber> AddRow(
        SequenceNumber readSequenceNumber,
        const ProtoValue& key,
        const ProtoValue& value,
        SequenceNumber writeSequenceNumber,
        MemoryTableOperationOutcomeTask operationOutcomeTask
    ) = 0;

    virtual shared_ptr<KeyComparer> GetKeyComparer(
    ) = 0;

    virtual shared_ptr<IMessageFactory> GetKeyFactory(
    ) = 0;

    virtual shared_ptr<IMessageFactory> GetValueFactory(
    ) = 0;

    virtual task<CheckpointNumber> Replay(
        const LoggedRowWrite& rowWrite
    ) = 0;

    virtual task<ReadResult> Read(
        const ReadRequest& readRequest
    ) = 0;

    virtual cppcoro::async_generator<EnumerateResult> Enumerate(
        const EnumerateRequest& readRequest
    ) = 0;

    virtual IndexNumber GetIndexNumber(
    ) const = 0;

    virtual const IndexName& GetIndexName(
    ) const = 0;

    virtual task<> Join(
    ) = 0;

    virtual task<LoggedCheckpoint> StartCheckpoint(
    ) = 0;

    virtual task<> Checkpoint(
        const LoggedCheckpoint& loggedCheckpoint,
        shared_ptr<IPartitionWriter> partitionWriter
    ) = 0;

    virtual task<> Replay(
        const LoggedCheckpoint& loggedCheckpoint
    ) = 0;

    virtual task<> UpdatePartitions(
        const LoggedCheckpoint& loggedCheckpoint,
        vector<shared_ptr<IPartition>> partitions
    ) = 0;

};

}