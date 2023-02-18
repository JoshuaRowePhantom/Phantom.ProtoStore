#include "MemoryTableImpl.h"
#include "Phantom.System/atomic.h"

namespace Phantom::ProtoStore
{

TransactionOutcome GetTransactionOutcome(
    MemoryTableOutcomeAndSequenceNumber value
)
{
    return static_cast<TransactionOutcome>(
        static_cast<std::uint64_t>(value)
        & static_cast<std::uint64_t>(MemoryTableOutcomeAndSequenceNumber::OutcomeMask));
}

SequenceNumber ToSequenceNumber(
    MemoryTableOutcomeAndSequenceNumber value
)
{
    return static_cast<SequenceNumber>(
        static_cast<std::uint64_t>(value)
        & static_cast<std::uint64_t>(MemoryTableOutcomeAndSequenceNumber::NumberMask));
}

MemoryTableTransactionOutcome ToMemoryTableTransactionOutcome(
    MemoryTableOutcomeAndSequenceNumber value)
{
    return
    {
        .Outcome = GetTransactionOutcome(value),
        .WriteSequenceNumber = ToSequenceNumber(value),
    };
}

MemoryTableOutcomeAndSequenceNumber ToMemoryTableOutcomeAndSequenceNumber(
    SequenceNumber sequenceNumber,
    TransactionOutcome transactionOutcome)
{
    return static_cast<MemoryTableOutcomeAndSequenceNumber>(
        static_cast<std::uint64_t>(sequenceNumber)
        | static_cast<std::uint64_t>(transactionOutcome));
}

MemoryTableOutcomeAndSequenceNumber MemoryTable::ToOutcomeUnknownSubsequentInsertion(
    SequenceNumber sequenceNumber)
{
    return static_cast<MemoryTableOutcomeAndSequenceNumber>(
        static_cast<std::uint64_t>(sequenceNumber)
        | static_cast<std::uint64_t>(MemoryTableOutcomeAndSequenceNumber::OutcomeUnknown));
}

MemoryTable::MemoryTable(
    const KeyComparer* keyComparer
)
    : 
    m_comparer(
        keyComparer
    ),
    m_skipList(
        m_comparer),
    m_earliestSequenceNumber(
        SequenceNumber::Latest
    ),
    m_latestSequenceNumber(
        SequenceNumber::Earliest
    )
{
}

MemoryTable::~MemoryTable()
{
    SyncDestroy();
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

task<std::optional<SequenceNumber>> MemoryTable::AddRow(
    SequenceNumber readSequenceNumber,
    MemoryTableRow& row,
    shared_ptr<DelayedMemoryTableTransactionOutcome> delayedTransactionOutcome
)
{
    InsertionKey insertionKey(
        row,
        delayedTransactionOutcome,
        readSequenceNumber);

    auto memoryTableWriteSequenceNumber = ToMemoryTableOutcomeAndSequenceNumber(
        row.WriteSequenceNumber,
        TransactionOutcome::Unknown);

    auto [iterator, succeeded] = m_skipList.insert(
        move(insertionKey));

    if (!succeeded)
    {
        // Do a cursory check to see what the outcome was.
        // We expect that most transactions commit.  The outcome is likely to be "Committed".
        // The cursory check for this is better than acquiring the mutex.
        auto previousMemoryTableWriteSequenceNumber = iterator->WriteSequenceNumber.load(
            std::memory_order_acquire);

        auto previousTransactionOutcome = GetTransactionOutcome(
            previousMemoryTableWriteSequenceNumber);

        // If this conflicting operation was committed,
        // then we return that information right away.
        if (previousTransactionOutcome == TransactionOutcome::Committed)
        {
            co_return ToSequenceNumber(previousMemoryTableWriteSequenceNumber);
        }

        // We'll need to lock the entry so that we can resolve it
        // and update it in place.
        auto lock = co_await iterator->Mutex.scoped_lock_async();

        // The cursory check shows the result is either Aborted or Unknown.
        // If unknown, force resolution of the transaction.
        if (previousTransactionOutcome == TransactionOutcome::Unknown)
        {
            auto resolution = co_await delayedTransactionOutcome->ResolveTargetTransaction(
                iterator->DelayedTransactionOutcome);
            previousTransactionOutcome = resolution.Outcome;
        }

        // Now we might discover there's a committed transaction.
        if (previousTransactionOutcome == TransactionOutcome::Committed)
        {
            co_return ToSequenceNumber(previousMemoryTableWriteSequenceNumber);
        }

        assert(previousTransactionOutcome == TransactionOutcome::Aborted);

        // Oh jolly joy!  The previous write aborted, so this one
        // can proceed.
        iterator->Row.WriteSequenceNumber = row.WriteSequenceNumber;
        iterator->Row.Value = std::move(row.Value);
        iterator->DelayedTransactionOutcome = delayedTransactionOutcome;

        memoryTableWriteSequenceNumber = ToOutcomeUnknownSubsequentInsertion(
            row.WriteSequenceNumber);

        iterator->WriteSequenceNumber.store(
            memoryTableWriteSequenceNumber,
            std::memory_order_release);

        // We now allow other potential resolvers to resolve.
        lock.unlock();
    }

    m_unresolvedRowCount.fetch_add(
        1,
        std::memory_order_acq_rel);

    spawn(
        UpdateOutcome(
            *iterator,
            // We pass in the parameter version of delayedTransactionOutcome
            // so that no 
            std::move(delayedTransactionOutcome)));

    co_return{};
}

task<> MemoryTable::UpdateOutcome(
    MemoryTableValue& memoryTableValue,
    shared_ptr<DelayedMemoryTableTransactionOutcome> delayedTransactionOutcome)
{
    auto outcome = co_await delayedTransactionOutcome->GetOutcome();

    auto lock = co_await memoryTableValue.Mutex.scoped_lock_async();
    // It's possible that another UpdateOutcome replaced the DelayedTransactionOutcome
    // because this row aborted and the other resolver updated the row.
    // 
    // Check for that condition before updating the row.
    if (delayedTransactionOutcome != memoryTableValue.DelayedTransactionOutcome)
    {
        lock.unlock();

        m_unresolvedRowCount.fetch_add(
            -1,
            std::memory_order_release);
        
        m_rowResolved.set();

        co_return;
    }

    memoryTableValue.Row.WriteSequenceNumber = outcome.WriteSequenceNumber;

    // After this point, 
    memoryTableValue.WriteSequenceNumber.store(
        ToMemoryTableOutcomeAndSequenceNumber(outcome.WriteSequenceNumber, outcome.Outcome),
        std::memory_order_release
    );

    if (outcome.Outcome == TransactionOutcome::Committed)
    {
        m_committedRowCount.fetch_add(
            1, 
            std::memory_order_release);
    }

    // We don't need the resolver anymore.
    memoryTableValue.DelayedTransactionOutcome = nullptr;
    
    lock.unlock();

    m_unresolvedRowCount.fetch_add(
        -1,
        std::memory_order_release);

    m_rowResolved.set();
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

row_generator MemoryTable::Enumerate(
    shared_ptr<DelayedMemoryTableTransactionOutcome> delayedTransactionOutcome,
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

        // The rowTransactionOutcome value needs to be acquired
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
        auto transactionOutcome = GetTransactionOutcome(
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
            if (transactionOutcome == TransactionOutcome::Unknown)
            {
                // We need to wait for resolution of this row.
                auto lock = co_await memoryTableValue.Mutex.scoped_lock_async();
                auto targetRowDelayedTransactionOutcome = memoryTableValue.DelayedTransactionOutcome;
                lock.unlock();

                if (delayedTransactionOutcome)
                {
                    transactionOutcome = (co_await delayedTransactionOutcome->ResolveTargetTransaction(
                        targetRowDelayedTransactionOutcome
                    )).Outcome;
                }
                else
                {
                    transactionOutcome = (co_await targetRowDelayedTransactionOutcome->GetOutcome())
                        .Outcome;
                }
                
                assert(transactionOutcome == TransactionOutcome::Aborted
                    || transactionOutcome == TransactionOutcome::Committed);
            }

            if (transactionOutcome == TransactionOutcome::Aborted)
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
                    .TransactionId = memoryTableValue.Row.TransactionId ? &*(memoryTableValue.Row.TransactionId) : nullptr,
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

SequenceNumber MemoryTable::GetLatestSequenceNumber(
)
{
    return m_latestSequenceNumber.load(
        std::memory_order_acquire);
}

void MemoryTable::UpdateSequenceNumberRange(
    SequenceNumber writeSequenceNumber
)
{
    compare_exchange_weak_transform(
        m_earliestSequenceNumber,
        [=](auto value)
    {
        return writeSequenceNumber < value 
            ? writeSequenceNumber
            : value;
    },
        std::memory_order_relaxed,
        std::memory_order_release,
        std::memory_order_relaxed);

    compare_exchange_weak_transform(
        m_latestSequenceNumber,
        [=](auto value)
    {
        return writeSequenceNumber > value
            ? writeSequenceNumber
            : value;
    },
        std::memory_order_relaxed,
        std::memory_order_release,
        std::memory_order_relaxed
        );
}

row_generator MemoryTable::Checkpoint()
{
    auto end = m_skipList.end();
    for (auto iterator = m_skipList.begin();
        iterator != end;
        ++iterator)
    {
        auto outcome = GetTransactionOutcome(iterator->WriteSequenceNumber.load(std::memory_order_acquire));

        if (outcome == TransactionOutcome::Committed)
        {
            co_yield ResultRow
            {
                .Key = iterator->Row.Key.get(),
                .WriteSequenceNumber = iterator->Row.WriteSequenceNumber,
                .Value = iterator->Row.Value.get(),
                .TransactionId = iterator->Row.TransactionId ? &*(iterator->Row.TransactionId) : nullptr,
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
        TransactionOutcome::Committed)
}
{
    assert(Row.Key.get());
    auto writeSequenceNumber = Row.WriteSequenceNumber;
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
            TransactionOutcome::Unknown)
    },
    DelayedTransactionOutcome{ other.DelayedTransactionOutcome }
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
    shared_ptr<DelayedMemoryTableTransactionOutcome>& delayedTransactionOutcome,
    SequenceNumber readSequenceNumber)
    :
    Row(row),
    DelayedTransactionOutcome(delayedTransactionOutcome),
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
        comparisonResult = key2.Row.WriteSequenceNumber <=> key1.Row.WriteSequenceNumber;
    }

    return comparisonResult;
}


DelayedMemoryTableTransactionOutcome::ScopedCompleter::ScopedCompleter(
    DelayedMemoryTableTransactionOutcome& delayedTransactionOutcome)
    :
    m_delayedTransactionOutcome{ delayedTransactionOutcome }
{
}

DelayedMemoryTableTransactionOutcome::ScopedCompleter::~ScopedCompleter()
{
    m_delayedTransactionOutcome.Complete();
}

DelayedMemoryTableTransactionOutcome::DelayedMemoryTableTransactionOutcome(
    MemoryTableTransactionSequenceNumber originatingTransactionSequenceNumber
) : m_originatingTransactionSequenceNumber(originatingTransactionSequenceNumber)
{
    m_outcomeTask = GetOutcomeImpl();
}

shared_task<MemoryTableTransactionOutcome> DelayedMemoryTableTransactionOutcome::GetOutcomeImpl()
{
    co_await m_resolvedSignal;
    co_return ToMemoryTableTransactionOutcome(
        m_outcomeAndSequenceNumber.load(std::memory_order_relaxed)
    );
}

shared_task<MemoryTableTransactionOutcome> DelayedMemoryTableTransactionOutcome::GetOutcome()
{
    return m_outcomeTask;
}

DelayedMemoryTableTransactionOutcome::ScopedCompleter DelayedMemoryTableTransactionOutcome::GetCompleter()
{
    return { *this };
}

MemoryTableTransactionOutcome DelayedMemoryTableTransactionOutcome::BeginCommit(
    SequenceNumber writeSequenceNumber)
{
    auto previousResult = MemoryTableOutcomeAndSequenceNumber::Earliest;
    auto committedResult = ToMemoryTableOutcomeAndSequenceNumber(writeSequenceNumber, TransactionOutcome::Committed);

    if (!m_outcomeAndSequenceNumber.compare_exchange_strong(
        previousResult,
        committedResult,
        std::memory_order_acq_rel))
    {
        return ToMemoryTableTransactionOutcome(previousResult);
    }

    return ToMemoryTableTransactionOutcome(committedResult);
}

void DelayedMemoryTableTransactionOutcome::Complete()
{
    auto previousValue = m_outcomeAndSequenceNumber.load(std::memory_order_acquire);
    if (previousValue == MemoryTableOutcomeAndSequenceNumber::Earliest)
    {
        m_outcomeAndSequenceNumber.compare_exchange_strong(
            previousValue,
            MemoryTableOutcomeAndSequenceNumber::OutcomeAborted,
            std::memory_order_release
        );
    }

    m_resolvedSignal.set();
}

task<MemoryTableTransactionOutcome> DelayedMemoryTableTransactionOutcome::ResolveTargetTransaction(
    shared_ptr<DelayedMemoryTableTransactionOutcome> targetTransaction)
{
    auto resolvingTransactionWriteLock = co_await m_deadlockDetectionLock.writer().scoped_lock_async();
    m_currentDeadlockDetectionResolutionTarget = std::move(targetTransaction);
    resolvingTransactionWriteLock.unlock();

    // Now search the list of transactions, looking for the latest transaction we find,
    // and looking for ourself to see if there's a loop.
    shared_ptr<DelayedMemoryTableTransactionOutcome> latestTransaction = m_currentDeadlockDetectionResolutionTarget;
    shared_ptr<DelayedMemoryTableTransactionOutcome> deadlockedTransaction = m_currentDeadlockDetectionResolutionTarget;
    while (
        deadlockedTransaction
        &&
        deadlockedTransaction.get() != this)
    {
        auto targetTransactionReadLock = co_await deadlockedTransaction->m_deadlockDetectionLock.reader().scoped_lock_async();
        deadlockedTransaction = deadlockedTransaction->m_currentDeadlockDetectionResolutionTarget;
        targetTransactionReadLock.unlock();

        if (deadlockedTransaction
            &&
            deadlockedTransaction->m_originatingTransactionSequenceNumber > latestTransaction->m_originatingTransactionSequenceNumber)
        {
            latestTransaction = deadlockedTransaction;
        }
    }

    if (deadlockedTransaction.get() == this)
    {
        // We found ourself in the list of deadlocked transactions, meaning we are involved in a loop.
        auto expectedOutcomeAndSequenceNumber = MemoryTableOutcomeAndSequenceNumber::Earliest;
        auto abortedResult = MemoryTableOutcomeAndSequenceNumber::OutcomeAborted;
        
        latestTransaction->m_outcomeAndSequenceNumber.compare_exchange_strong(
            expectedOutcomeAndSequenceNumber,
            abortedResult
        );

        latestTransaction->m_resolvedSignal.set();
    }

    auto result = co_await m_currentDeadlockDetectionResolutionTarget->GetOutcome();

    resolvingTransactionWriteLock = co_await m_deadlockDetectionLock.writer().scoped_lock_async();
    m_currentDeadlockDetectionResolutionTarget = nullptr;
    resolvingTransactionWriteLock.unlock();

    co_return result;
}

}
