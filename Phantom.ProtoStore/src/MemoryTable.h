#pragma once

#include "StandardTypes.h"
#include <cppcoro/async_generator.hpp>
#include <any>

namespace Phantom::ProtoStore
{

struct MemoryTableRow
{
    unique_ptr<Message> Key;
    SequenceNumber SequenceNumber;
    unique_ptr<Message> Value;
    TransactionId* TransactionId;
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
    // If there is a conflict, an exception of the proper type with be thrown,
    // and the row will be left untouched.
    // Otherwise, the content of the row are std::move'd into the memory table.
    virtual task<> AddRow(
        SequenceNumber readSequenceNumber,
        MemoryTableRow& row
    ) = 0;

    virtual cppcoro::async_generator<const MemoryTableRow*> Enumerate(
        SequenceNumber transactionReadSequenceNumber,
        SequenceNumber requestedReadSequenceNumber,
        KeyRangeEnd low,
        KeyRangeEnd high
    ) = 0;
};
}