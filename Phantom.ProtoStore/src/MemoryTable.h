#pragma once

#include "StandardTypes.h"
#include <cppcoro/async_generator.hpp>
#include "Phantom.Coroutines/async_manual_reset_event.h"

namespace Phantom::ProtoStore
{

struct MemoryTableRow
{
    unique_ptr<const Message> Key;
    SequenceNumber WriteSequenceNumber;
    unique_ptr<const Message> Value;
    std::optional<TransactionId> TransactionId;
};

struct KeyRangeEnd
{
    const Message* Key;
    Inclusivity Inclusivity;
};

struct MemoryTableOperationOutcome
{
    OperationOutcome Outcome;
    SequenceNumber WriteSequenceNumber;

    friend bool operator==(
        const MemoryTableOperationOutcome&,
        const MemoryTableOperationOutcome&
        ) = default;
};

enum class MemoryTableOutcomeAndSequenceNumber
{
    Earliest = 0,
    NumberMask = 0xfffffffffffffffc,
    OutcomeMask = 0x3,
    // These values must match those in OperationOutcome.
    OutcomeUnknown = 0x0,
    OutcomeCommitted = 0x1,
    OutcomeAborted = 0x2,
    // This value indicates that the value may not be atomically replaced.
    OutcomeUnknownSubsequentInsertion = 0x3,
};

MemoryTableOperationOutcome ToMemoryTableOperationOutcome(
    MemoryTableOutcomeAndSequenceNumber);

MemoryTableOutcomeAndSequenceNumber ToMemoryTableOutcomeAndSequenceNumber(
    SequenceNumber sequenceNumber,
    OperationOutcome operationOutcome);

using MemoryTableTransactionSequenceNumber = uint64_t;

class DelayedMemoryTableOperationOutcome
{
    MemoryTableTransactionSequenceNumber m_originatingTransactionSequenceNumber;
    std::atomic<MemoryTableOutcomeAndSequenceNumber> m_outcomeAndSequenceNumber = MemoryTableOutcomeAndSequenceNumber::Earliest;
    Phantom::Coroutines::async_manual_reset_event<> m_resolvedSignal;
    shared_task<MemoryTableOperationOutcome> m_outcomeTask;
    shared_task<MemoryTableOperationOutcome> GetOutcomeImpl();

public:
    DelayedMemoryTableOperationOutcome(
        MemoryTableTransactionSequenceNumber originatingTransactionSequenceNumber
    );

    shared_task<MemoryTableOperationOutcome> GetOutcome();
    shared_task<MemoryTableOperationOutcome> Resolve(
        MemoryTableTransactionSequenceNumber resolvingTransactionSequenceNumber);

    MemoryTableOperationOutcome Commit(
        SequenceNumber writeSequenceNumber);
    void Abort();
};

class IMemoryTable
    :
    public virtual IJoinable
{
public:
    virtual task<size_t> GetRowCount(
    ) = 0;

    // Add the specified row.
    // If there is a conflict, the sequence number of the conflicting row is returned.
    // Otherwise, the content of the row are std::move'd into the memory table.
    virtual task<std::optional<SequenceNumber>> AddRow(
        SequenceNumber readSequenceNumber,
        MemoryTableRow& row,
        MemoryTableOperationOutcomeTask task
    ) = 0;

    // Add the specified row, unconditionally.
    virtual task<> ReplayRow(
        MemoryTableRow& row
    ) = 0;

    virtual row_generator Enumerate(
        SequenceNumber readSequenceNumber,
        KeyRangeEnd low,
        KeyRangeEnd high
    ) = 0;

    virtual row_generator Checkpoint(
    ) = 0;

    virtual SequenceNumber GetLatestSequenceNumber(
    ) = 0;

    virtual task<optional<SequenceNumber>> CheckForWriteConflict(
        SequenceNumber readSequenceNumber,
        SequenceNumber writeSequenceNumber,
        const Message* key
    ) = 0;
};
}