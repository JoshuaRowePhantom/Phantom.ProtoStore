#pragma once

#include "StandardTypes.h"
#include "MemoryTable.h"

namespace Phantom::ProtoStore
{

class IPartition
{
public:
    virtual cppcoro::async_generator<ResultRow> Enumerate(
        SequenceNumber readSequenceNumber,
        KeyRangeEnd low,
        KeyRangeEnd high
    ) = 0;
};
}