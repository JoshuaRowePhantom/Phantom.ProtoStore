#pragma once

#include "StandardTypes.h"
#include "MemoryTable.h"

namespace Phantom::ProtoStore
{

class IPartition
{
public:
    virtual task<size_t> GetRowCount(
    ) = 0;

    virtual async_generator<ResultRow> Read(
        SequenceNumber readSequenceNumber,
        const Message* key,
        ReadValueDisposition readValueDisposition
    ) = 0;

    virtual async_generator<ResultRow> Enumerate(
        SequenceNumber readSequenceNumber,
        KeyRangeEnd low,
        KeyRangeEnd high,
        ReadValueDisposition readValueDisposition
    ) = 0;

    virtual SequenceNumber GetLatestSequenceNumber(
    ) = 0;

    virtual task<optional<SequenceNumber>> CheckForWriteConflict(
        SequenceNumber readSequenceNumber,
        const Message* key
    ) = 0;
};
}