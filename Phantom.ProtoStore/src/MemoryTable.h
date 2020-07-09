#pragma once

#include "StandardTypes.h"
#include <cppcoro/async_generator.hpp>
#include <any>

namespace Phantom::ProtoStore
{

class MemoryTableRow
{
    const Message* Key;
    SequenceNumber SequenceNumber;
    const Message* Value;
    const TransactionId* TransactionId;
};

struct KeyRangeEnd
{
    const Message* Value;
    Inclusivity Inclusivity;
};

class IMemoryTable
{
public:
    // Add the specified row.
    // If there is a conflict, an exception of the proper type with be thrown.
    virtual task<> AddRow(
        SequenceNumber readSequenceNumber,
        MemoryTableRow row
    ) = 0;

    virtual cppcoro::async_generator<const MemoryTableRow*> Enumerate(
        SequenceNumber transactionReadSequenceNumber,
        SequenceNumber requestedReadSequenceNumber,
        KeyRangeEnd low,
        KeyRangeEnd high
    ) = 0;
};
}