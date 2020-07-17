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

    virtual task<CheckpointNumber> Replay(
        const LoggedRowWrite& rowWrite
    ) = 0;

    virtual task<ReadResult> Read(
        const ReadRequest& readRequest
    ) = 0;

    virtual IndexNumber GetIndexNumber(
    ) const = 0;

    virtual const IndexName& GetIndexName(
    ) const = 0;

    virtual task<> Join(
    ) = 0;
};

}
