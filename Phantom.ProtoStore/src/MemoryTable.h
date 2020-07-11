#pragma once

#include "StandardTypes.h"
#include <cppcoro/async_generator.hpp>
#include <cppcoro/shared_task.hpp>

namespace Phantom::ProtoStore
{

struct MemoryTableRow
{
    unique_ptr<const Message> Key;
    SequenceNumber WriteSequenceNumber;
    unique_ptr<const Message> Value;
};

struct KeyRangeEnd
{
    const Message* Key;
    Inclusivity Inclusivity;
};

class IMemoryTable
{
public:
    typedef cppcoro::shared_task<OperationOutcome> OperationOutcomeTask;

    // Add the specified row.
    // If there is a conflict, an exception of the proper type with be thrown,
    // and the row will be left untouched.
    // Otherwise, the content of the row are std::move'd into the memory table.
    virtual task<> AddRow(
        SequenceNumber readSequenceNumber,
        MemoryTableRow& row,
        OperationOutcomeTask operationOutcome
    ) = 0;

    virtual cppcoro::async_generator<const MemoryTableRow*> Enumerate(
        SequenceNumber readSequenceNumber,
        KeyRangeEnd low,
        KeyRangeEnd high
    ) = 0;
};
}