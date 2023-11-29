#pragma once

#include "StandardTypes.h"
#include <cppcoro/async_generator.hpp>
#include "Phantom.Coroutines/async_manual_reset_event.h"
#include "Phantom.Coroutines/async_reader_writer_lock.h"
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"

namespace Phantom::ProtoStore
{

struct KeyRangeEnd
{
    ProtoValue Key;
    Inclusivity Inclusivity;
    std::optional<FieldId> LastFieldId;
};

struct MemoryTableTransactionOutcome
{
    TransactionOutcome Outcome;
    SequenceNumber WriteSequenceNumber;

    friend bool operator==(
        const MemoryTableTransactionOutcome&,
        const MemoryTableTransactionOutcome&
        ) = default;
};

enum class MemoryTableOutcomeAndSequenceNumber : uint64_t
{
    Earliest = 0,
    NumberMask = 0xfffffffffffffffc,
    OutcomeMask = 0x3,
    // These values must match those in TransactionOutcome.
    OutcomeUnknown = 0x0,
    OutcomeCommitted = 0x1,
    OutcomeAborted = 0x2,
};

MemoryTableTransactionOutcome ToMemoryTableTransactionOutcome(
    MemoryTableOutcomeAndSequenceNumber);

MemoryTableOutcomeAndSequenceNumber ToMemoryTableOutcomeAndSequenceNumber(
    SequenceNumber sequenceNumber,
    TransactionOutcome transactionOutcome);

constexpr MemoryTableTransactionSequenceNumber MemoryTableTransactionSequenceNumber_AbortAll = 0;
constexpr MemoryTableTransactionSequenceNumber MemoryTableTransactionSequenceNumber_ResolveAll = std::numeric_limits<uint64_t>::max();

class DelayedMemoryTableTransactionOutcome
{
    MemoryTableTransactionSequenceNumber m_originatingTransactionSequenceNumber;
    std::atomic<MemoryTableOutcomeAndSequenceNumber> m_outcomeAndSequenceNumber = MemoryTableOutcomeAndSequenceNumber::Earliest;
    Phantom::Coroutines::async_manual_reset_event<> m_resolvedSignal;
    shared_task<MemoryTableTransactionOutcome> m_outcomeTask;
    
    Phantom::Coroutines::async_reader_writer_lock<> m_deadlockDetectionLock;
    shared_ptr<DelayedMemoryTableTransactionOutcome> m_currentDeadlockDetectionResolutionTarget;

    shared_task<MemoryTableTransactionOutcome> GetOutcomeImpl();

    shared_task<MemoryTableTransactionOutcome> ResolveTargetTransactionImpl(
        shared_ptr<DelayedMemoryTableTransactionOutcome> targetTransaction);


public:
    DelayedMemoryTableTransactionOutcome(
        MemoryTableTransactionSequenceNumber originatingTransactionSequenceNumber
    );

    void Complete();
    shared_task<MemoryTableTransactionOutcome> GetOutcome();

    task<MemoryTableTransactionOutcome> ResolveTargetTransaction(
        shared_ptr<DelayedMemoryTableTransactionOutcome> targetTransaction);

    // Begin the process of committing the transaction.
    // Once this process has started, the transaction cannot be aborted.
    MemoryTableTransactionOutcome BeginCommit(
        SequenceNumber writeSequenceNumber);
};

class IMemoryTable
    :
    public virtual IJoinable
{
public:
    using Row = FlatMessage<FlatBuffers::LoggedRowWrite>;

    // Get the approximate number of rows in the memory table.
    // Rows that were rolled back may be included in the count.
    virtual size_t GetApproximateRowCount(
    ) = 0;

    // Add the specified row.
    // If there is a conflict, the sequence number of the conflicting row is returned.
    // Otherwise, the content of the row are std::move'd into the memory table.
    virtual task<std::optional<SequenceNumber>> AddRow(
        SequenceNumber readSequenceNumber,
        Row row,
        shared_ptr<DelayedMemoryTableTransactionOutcome> outcome
    ) = 0;

    // Add the specified row, unconditionally.
    virtual task<> ReplayRow(
        Row row
    ) = 0;

    virtual row_generator Enumerate(
        const shared_ptr<DelayedMemoryTableTransactionOutcome>& delayedTransactionOutcome,
        SequenceNumber readSequenceNumber,
        KeyRangeEnd low,
        KeyRangeEnd high
    ) = 0;

    // Given a key, return a SequenceNumber of any write conflicts.
    virtual task<optional<SequenceNumber>> CheckForWriteConflict(
        const shared_ptr<DelayedMemoryTableTransactionOutcome>& delayedTransactionOutcome,
        SequenceNumber readSequenceNumber,
        const ProtoValue& key
    ) = 0;

    virtual row_generator Checkpoint(
    ) = 0;

    virtual SequenceNumber GetLatestSequenceNumber(
    ) = 0;
};

std::shared_ptr<IMemoryTable> MakeMemoryTable(
    shared_ptr<const Schema> schema,
    shared_ptr<const ValueComparer> keyComparer
);

}