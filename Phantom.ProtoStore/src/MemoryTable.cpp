#include "MemoryTableImpl.h"

namespace Phantom::ProtoStore
{

enum class MemoryTable::MemoryTableOutcomeAndSequenceNumber
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

MemoryTable::MemoryTableOutcomeAndSequenceNumber MemoryTable::ToMemoryTableOutcomeAndSequenceNumber(
    SequenceNumber sequenceNumber,
    OperationOutcome operationOutcome)
{
    return static_cast<MemoryTableOutcomeAndSequenceNumber>(
        static_cast<std::uint64_t>(sequenceNumber)
        | static_cast<std::uint64_t>(operationOutcome));
}

MemoryTable::MemoryTableOutcomeAndSequenceNumber MemoryTable::ToOutcomeUnknownSubsequentInsertion(
    SequenceNumber sequenceNumber)
{
    return static_cast<MemoryTableOutcomeAndSequenceNumber>(
        static_cast<std::uint64_t>(sequenceNumber)
        | static_cast<std::uint64_t>(MemoryTableOutcomeAndSequenceNumber::OutcomeUnknownSubsequentInsertion));
}

OperationOutcome MemoryTable::GetOperationOutcome(
    MemoryTableOutcomeAndSequenceNumber value
)
{
    return static_cast<OperationOutcome>(
        static_cast<std::uint64_t>(value)
        & static_cast<std::uint64_t>(MemoryTableOutcomeAndSequenceNumber::OutcomeMask));
}

SequenceNumber MemoryTable::ToSequenceNumber(
    MemoryTableOutcomeAndSequenceNumber value
)
{
    return static_cast<SequenceNumber>(
        static_cast<std::uint64_t>(value)
        & static_cast<std::uint64_t>(MemoryTableOutcomeAndSequenceNumber::NumberMask));
}

MemoryTable::MemoryTable(
    const KeyComparer* keyComparer
)
    : 
    m_comparer(
        keyComparer
    ),
    m_skipList(
        m_comparer)
{
}

task<size_t> MemoryTable::GetRowCount()
{
    while (m_unresolvedRowCount.load(
        std::memory_order_acquire))
    {
        co_await m_rowResolved;
    }

    co_return m_committedRowCount.load(
        std::memory_order_relaxed);
}

task<> MemoryTable::AddRow(
    SequenceNumber readSequenceNumber,
    MemoryTableRow& row,
    MemoryTableOperationOutcomeTask asyncOperationOutcome
)
{
    InsertionKey insertionKey(
        row,
        asyncOperationOutcome,
        readSequenceNumber);

    auto [iterator, succeeded] = m_skipList.insert(
        move(insertionKey));

    auto memoryTableWriteSequenceNumber = ToMemoryTableOutcomeAndSequenceNumber(
        row.WriteSequenceNumber,
        OperationOutcome::Unknown);

    if (succeeded)
    {
        m_unresolvedRowCount.fetch_add(
            1,
            std::memory_order_acq_rel);

        bool updateRowCounts = true;

        // Use the transaction resolver without acquiring a mutex.
        m_asyncScope.spawn(
            ResolveMemoryTableRowOutcome(
                memoryTableWriteSequenceNumber,
                *iterator,
                asyncOperationOutcome,
                updateRowCounts));
    }
    else
    {
        // Do a cursory check to see what the outcome was.
        // We expect that most transactions commit.  The outcome is likely to be "Committed".
        // The cursory check for this is better than acquiring the mutex.
        auto previousMemoryTableWriteSequenceNumber = iterator->WriteSequenceNumber.load(
            std::memory_order_relaxed);

        auto previousOperationOutcome = GetOperationOutcome(
            previousMemoryTableWriteSequenceNumber);

        if (previousOperationOutcome == OperationOutcome::Committed)
        {
            throw WriteConflict();
        }

        // The cursory check shows the result is either Aborted or Unknown.
        // Since we'll want to check the operation outcome's task,
        // then replace the Value and Task, we acquire the lock now.
        auto lock = co_await iterator->Mutex.scoped_lock_async();

        // This should be fast if the transaction outcome is known,
        // so there's no point in re-checking the fast outcome variable.
        // If the outcome wasn't known, we'll have to wait anyway.
        bool updateRowCounts = false;
        previousOperationOutcome = co_await ResolveMemoryTableRowOutcome(
            previousMemoryTableWriteSequenceNumber,
            *iterator,
            iterator->AsyncOperationOutcome,
            updateRowCounts);

        // Now we might discover there's a committed transaction.
        if (previousOperationOutcome == OperationOutcome::Committed)
        {
            throw WriteConflict();
        }

        assert(previousOperationOutcome == OperationOutcome::Aborted);

        // Oh jolly joy!  The previous write aborted, so this one
        // can proceed.
        m_unresolvedRowCount.fetch_add(
            1,
            std::memory_order_acq_rel);

        iterator->Row.WriteSequenceNumber = row.WriteSequenceNumber;
        iterator->Row.Value = std::move(row.Value);
        iterator->AsyncOperationOutcome = asyncOperationOutcome;

        memoryTableWriteSequenceNumber = ToOutcomeUnknownSubsequentInsertion(
            row.WriteSequenceNumber);

        iterator->WriteSequenceNumber.store(
            memoryTableWriteSequenceNumber,
            std::memory_order_release);


        // Use the transaction resolve that acquires the mutex.
        updateRowCounts = true;
        m_asyncScope.spawn(
            ResolveMemoryTableRowOutcome(
                *iterator,
                updateRowCounts));
    }

    co_return;
}

task<> MemoryTable::ReplayRow(
    MemoryTableRow& row
)
{
    ReplayInsertionKey replayKey(
        row);

    auto [iterator, succeeded] = m_skipList.insert(
        move(replayKey));

    m_committedRowCount.fetch_add(
        1,
        std::memory_order_relaxed);

    assert(succeeded);

    co_return;
}

cppcoro::async_generator<ResultRow> MemoryTable::Enumerate(
    SequenceNumber readSequenceNumber,
    KeyRangeEnd low,
    KeyRangeEnd high
)
{
    EnumerationKey enumerationKey
    {
        .KeyLow = low.Key,
        .KeyLowInclusivity = low.Inclusivity,
        .ReadSequenceNumber = readSequenceNumber,
    };

    auto [findIterator, keyComparisonResult] = m_skipList.find(
        enumerationKey);

    while (findIterator)
    {
        auto& memoryTableValue = *findIterator;

        auto highComparisonResult = m_comparer(
            memoryTableValue,
            high);

        if (highComparisonResult == std::weak_ordering::greater)
        {
            co_return;
        }

        // No matter what, the next key to enumerate will be at least as large
        // as the current key.
        enumerationKey.KeyLow = memoryTableValue.Row.Key.get();

        // The rowOperationOutcome value needs to be acquired
        // before reading the sequence number for return determination.
        // It was good enough for searching, but the transaction outcome
        // might have changed and the sequence number increased
        // after locating this row, in which case we should not return it.
        // Since the write sequence number being too high is expected
        // to be the rare case, i.e. readers are reading recently committed rows,
        // acquire the WriteSequenceNumber now.
        auto memoryTableWriteSequenceNumber = memoryTableValue.WriteSequenceNumber.load(
            std::memory_order_acquire);

        auto writeSequenceNumber = ToSequenceNumber(
            memoryTableWriteSequenceNumber);
        auto operationOutcome = GetOperationOutcome(
            memoryTableWriteSequenceNumber);

        if (writeSequenceNumber > readSequenceNumber)
        {
            // We found a version of the row later than requested.
            // The actual outcome doesn't matter, because we wouldn't have returned the row anyway.

            // Change the enumeration key to be inclusive so that we'll
            // locate the highest sequence number for the row that
            // is lower than the requested sequence number.
            // As a hint to the skip list, we can increment the iterator to do one less comparison.
            ++findIterator;
            enumerationKey.KeyLowInclusivity = Inclusivity::Inclusive;
            enumerationKey.SequenceNumberToSkipForKeyLow = writeSequenceNumber;
        }
        // We potentially found a version of the row to return, but we have to check its outcome.
        else
        {
            if (operationOutcome == OperationOutcome::Unknown)
            {
                // We need to wait for resolution of this row.
                bool updateRowCounts = false;
                operationOutcome = co_await ResolveMemoryTableRowOutcome(
                    memoryTableValue,
                    updateRowCounts);
                
                assert(operationOutcome == OperationOutcome::Aborted
                    || operationOutcome == OperationOutcome::Committed);
            }

            if (operationOutcome == OperationOutcome::Aborted)
            {
                // We found a version of the row later than requested.
                // Change the enumeration key to be inclusive so that we'll
                // locate the highest sequence number for the row that
                // is lower than the requested sequence number.
                // As a hint to the skip list, we can increment the iterator to do one less comparison.
                ++findIterator;
                enumerationKey.KeyLowInclusivity = Inclusivity::Inclusive;
                enumerationKey.SequenceNumberToSkipForKeyLow = writeSequenceNumber;
            }
            else
            {
                // The row resolved as Committed and the write sequence number is good.
                // Yield it to the caller.
                co_yield ResultRow
                {
                    .Key = memoryTableValue.Row.Key.get(),
                    .WriteSequenceNumber = memoryTableValue.Row.WriteSequenceNumber,
                    .Value = memoryTableValue.Row.Value.get(),
                };

                // Change the enumeration key to be exclusive so that we'll
                // skip all lower sequence numbers for the same row.
                // As a hint to the skip list, we can increment the iterator to do one less comparison.
                ++findIterator;
                enumerationKey.KeyLowInclusivity = Inclusivity::Exclusive;
                enumerationKey.SequenceNumberToSkipForKeyLow.reset();
            }
        }

        keyComparisonResult = m_skipList.find_in_place(
            enumerationKey,
            findIterator
        );
    }
}

task<optional<SequenceNumber>> MemoryTable::CheckForWriteConflict(
    SequenceNumber readSequenceNumber,
    const Message* key
)
{
    KeyRangeEnd keyLow
    {
        .Key = key,
        .Inclusivity = Inclusivity::Inclusive,
    };

    auto generator = Enumerate(
        SequenceNumber::Latest,
        keyLow,
        keyLow
    );

    for (auto iterator = co_await generator.begin();
        iterator != generator.end();
        co_await ++iterator)
    {
        if ((*iterator).WriteSequenceNumber > readSequenceNumber)
        {
            co_return (*iterator).WriteSequenceNumber;
        }

        co_return optional<SequenceNumber>();
    }
    co_return optional<SequenceNumber>();
}

task<OperationOutcome> MemoryTable::ResolveMemoryTableRowOutcome(
    MemoryTableOutcomeAndSequenceNumber writeSequenceNumber,
    MemoryTableValue& memoryTableValue,
    MemoryTableOperationOutcomeTask task,
    bool updateRowCounters
)
{
    auto memoryTableOperationOutcome = co_await task;

    assert(memoryTableOperationOutcome.Outcome == OperationOutcome::Aborted
        || memoryTableOperationOutcome.Outcome == OperationOutcome::Committed);

    auto newWriteSequenceNumber = ToMemoryTableOutcomeAndSequenceNumber(
        memoryTableOperationOutcome.WriteSequenceNumber,
        memoryTableOperationOutcome.Outcome);

    if (memoryTableOperationOutcome.Outcome == OperationOutcome::Committed)
    {
        // The completed operation must happen at a sequence number at least as
        // late as the current row, or snapshot isolation will have failed.
        assert(memoryTableOperationOutcome.WriteSequenceNumber >= ToSequenceNumber(memoryTableValue.WriteSequenceNumber.load(std::memory_order_relaxed)));

        // The transaction committed, and we can safely write the 
        // completion information to the transaction.
        memoryTableValue.Row.WriteSequenceNumber = memoryTableOperationOutcome.WriteSequenceNumber;

        memoryTableValue.WriteSequenceNumber.store(
            newWriteSequenceNumber,
            std::memory_order_release);

        if (updateRowCounters)
        {
            m_unresolvedRowCount.fetch_sub(
                1,
                std::memory_order_relaxed);
            m_committedRowCount.fetch_add(
                1,
                std::memory_order_relaxed);
            m_rowResolved.set();
        }
    }
    else
    {
        if (updateRowCounters)
        {
            m_unresolvedRowCount.fetch_sub(
                1,
                std::memory_order_relaxed);
            m_rowResolved.set();
        }

        // The transaction aborted.  Some other resolver may
        // have noticed and may have already written the outcome,
        // and yet another AddRow might have replaced the outcome,
        // so conditionally write the outcome if it is still Unknown.
        // In those cases, AddRow will have used an outcome of OutcomeUnknownSubsequentInsertion.
        memoryTableValue.WriteSequenceNumber.compare_exchange_strong(
            writeSequenceNumber,
            newWriteSequenceNumber,
            std::memory_order_relaxed);
    }

    co_return memoryTableOperationOutcome.Outcome;
}


task<OperationOutcome> MemoryTable::ResolveMemoryTableRowOutcome(
    MemoryTableValue& memoryTableValue,
    bool updateRowCounters
)
{
    auto lock = co_await memoryTableValue.Mutex.scoped_lock_async();
    auto writeSequenceNumber = memoryTableValue.WriteSequenceNumber.load(
        std::memory_order_acquire);

    auto outcome = GetOperationOutcome(
        writeSequenceNumber);

    if (outcome != OperationOutcome::Unknown)
    {
        co_return outcome;
    }

    co_return co_await ResolveMemoryTableRowOutcome(
        writeSequenceNumber,
        memoryTableValue,
        MemoryTableOperationOutcomeTask(memoryTableValue.AsyncOperationOutcome),
        updateRowCounters);
}

cppcoro::async_generator<ResultRow> MemoryTable::Checkpoint()
{
    auto end = m_skipList.end();
    for (auto iterator = m_skipList.begin();
        iterator != end;
        ++iterator)
    {
        bool updateRowCounts = false;
        auto outcome = co_await ResolveMemoryTableRowOutcome(
            *iterator,
            updateRowCounts);

        if (outcome == OperationOutcome::Committed)
        {
            co_yield ResultRow
            {
                .Key = iterator->Row.Key.get(),
                .WriteSequenceNumber = iterator->Row.WriteSequenceNumber,
                .Value = iterator->Row.Value.get(),
            };
        }
    }
}

MemoryTable::MemoryTableValue::MemoryTableValue(
    ReplayInsertionKey&& other
)
    :
    Row{ move(other.Row) },
    WriteSequenceNumber
{
    ToMemoryTableOutcomeAndSequenceNumber(
        other.Row.WriteSequenceNumber,
        OperationOutcome::Committed)
}
{
    assert(Row.Key.get());
    auto writeSequenceNumber = Row.WriteSequenceNumber;

    AsyncOperationOutcome = [writeSequenceNumber]() -> MemoryTableOperationOutcomeTask
    {
        co_return MemoryTableOperationOutcome
        {
            .Outcome = OperationOutcome::Committed,
            .WriteSequenceNumber = writeSequenceNumber,
        };
    }();
}

MemoryTable::MemoryTableValue::MemoryTableValue(
    InsertionKey&& other
)
    :
    Row { move(other.Row) },
    WriteSequenceNumber 
    {
        ToMemoryTableOutcomeAndSequenceNumber(
            other.Row.WriteSequenceNumber,
            OperationOutcome::Unknown)
    },
    AsyncOperationOutcome { other.AsyncOperationOutcome }
{
    assert(Row.Key.get());
}

MemoryTable::ReplayInsertionKey::ReplayInsertionKey(
    MemoryTableRow& row
) :
    Row(row)
{}

MemoryTable::InsertionKey::InsertionKey(
    MemoryTableRow& row,
    MemoryTableOperationOutcomeTask memoryTableOperationOutcomeTask,
    SequenceNumber readSequenceNumber)
    :
    Row(row),
    AsyncOperationOutcome(memoryTableOperationOutcomeTask),
    ReadSequenceNumber(readSequenceNumber)
{
}

MemoryTable::InsertionKey& MemoryTable::InsertionKey::InsertionKey::operator=(
    MemoryTableValue&& memoryTableValue)
{
    Row = move(memoryTableValue.Row);
    return *this;
}


std::weak_ordering MemoryTable::MemoryTableRowComparer::operator()(
    const MemoryTableValue& key1,
    const MemoryTableValue& key2
    ) const
{
    auto comparisonResult = (*m_keyComparer)(
        key1.Row.Key.get(),
        key2.Row.Key.get());

    return comparisonResult;
}

std::weak_ordering MemoryTable::MemoryTableRowComparer::operator()(
    const MemoryTableValue& key1,
    const InsertionKey& key2
    ) const
{
    auto comparisonResult = (*m_keyComparer)(
        key1.Row.Key.get(),
        key2.Row.Key.get());

    if (comparisonResult != std::weak_ordering::equivalent)
    {
        return comparisonResult;
    }

    auto sequenceNumber = key1.Row.WriteSequenceNumber;

    if (sequenceNumber > key2.ReadSequenceNumber
        ||
        sequenceNumber >= key2.Row.WriteSequenceNumber)
    {
        // By returning equivalent here, the SkipList
        // won't insert the row.
        return std::weak_ordering::equivalent;
    }

    return std::weak_ordering::greater;
}

std::weak_ordering MemoryTable::MemoryTableRowComparer::operator()(
    const MemoryTableValue& key1,
    const EnumerationKey& key2
    ) const
{
    auto comparisonResult = 
        !key2.KeyLow
        ?
        std::weak_ordering::greater
        :
        (*m_keyComparer)(
            key1.Row.Key.get(),
            key2.KeyLow);

    if (comparisonResult == std::weak_ordering::equivalent
        &&
        key2.KeyLowInclusivity == Inclusivity::Exclusive)
    {
        return std::weak_ordering::less;
    }

    if (comparisonResult != std::weak_ordering::equivalent)
    {
        return comparisonResult;
    }

    auto sequenceNumber = key1.Row.WriteSequenceNumber;

    if (comparisonResult == std::weak_ordering::equivalent
        &&
        key2.SequenceNumberToSkipForKeyLow.has_value()
        &&
        key2.SequenceNumberToSkipForKeyLow <= sequenceNumber)
    {
        return std::weak_ordering::less;
    }

    if (sequenceNumber > key2.ReadSequenceNumber)
    {
        return std::weak_ordering::less;
    }

    if (sequenceNumber == key2.ReadSequenceNumber)
    {
        return std::weak_ordering::equivalent;
    }

    return std::weak_ordering::greater;
}

std::weak_ordering MemoryTable::MemoryTableRowComparer::operator()(
    const MemoryTableValue& key1,
    const KeyRangeEnd& key2
    ) const
{
    auto comparisonResult =
        !key2.Key
        ?
        std::weak_ordering::less
        :
        (*m_keyComparer)(
            key1.Row.Key.get(),
            key2.Key);

    if (comparisonResult == std::weak_ordering::equivalent
        &&
        key2.Inclusivity == Inclusivity::Exclusive)
    {
        return std::weak_ordering::greater;
    }

    return comparisonResult;
}

std::weak_ordering MemoryTable::MemoryTableRowComparer::operator()(
    const MemoryTableValue& key1,
    const ReplayInsertionKey& key2
    ) const
{
    auto comparisonResult =
        (*m_keyComparer)(
            key1.Row.Key.get(),
            key2.Row.Key.get()
            );

    if (comparisonResult == std::weak_ordering::equivalent)
    {
        comparisonResult = key1.Row.WriteSequenceNumber <=> key2.Row.WriteSequenceNumber;
    }

    return comparisonResult;
}

}