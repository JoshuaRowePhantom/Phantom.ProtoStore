#include "MemoryTableImpl.h"

namespace Phantom::ProtoStore
{

MemoryTable::MemoryTable(
    const KeyComparer* keyComparer
)
    : 
    m_comparer(
        keyComparer
    ),
    m_skipList(
        m_comparer)
{}

MemoryTable::~MemoryTable()
{}

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

    if (!succeeded)
    {
        throw WriteConflict();
    }

    m_asyncScope.spawn(
        ResolveMemoryTableRowOutcome(
            *iterator,
            asyncOperationOutcome));

    co_return;
}

cppcoro::async_generator<const MemoryTableRow*> MemoryTable::Enumerate(
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
        auto rowOperationOutcome = memoryTableValue.OperationOutcome.load(
            std::memory_order_acquire);

        if (memoryTableValue.WriteSequenceNumber > readSequenceNumber)
        {
            // We found a version of the row later than requested.
            // The actual outcome doesn't matter, because we wouldn't have returned the row anyway.

            // Change the enumeration key to be inclusive so that we'll
            // locate the highest sequence number for the row that
            // is lower than the requested sequence number.
            // As a hint to the skip list, we can increment the iterator to do one less comparison.
            ++findIterator;
            enumerationKey.KeyLowInclusivity = Inclusivity::Inclusive;
            enumerationKey.SequenceNumberToSkipForKeyLow = memoryTableValue.WriteSequenceNumber;
        }
        // We potentially found a version of the row to return, but we have to check its outcome.
        else
        {
            if (rowOperationOutcome == OperationOutcome::Unknown)
            {
                // We need to wait for resolution of this row.
                rowOperationOutcome = co_await ResolveMemoryTableRowOutcome(
                    memoryTableValue);
                
                assert(rowOperationOutcome == OperationOutcome::Aborted
                    || rowOperationOutcome == OperationOutcome::Committed);
            }

            if (rowOperationOutcome == OperationOutcome::Aborted)
            {
                // We found a version of the row later than requested.
                // Change the enumeration key to be inclusive so that we'll
                // locate the highest sequence number for the row that
                // is lower than the requested sequence number.
                // As a hint to the skip list, we can increment the iterator to do one less comparison.
                ++findIterator;
                enumerationKey.KeyLowInclusivity = Inclusivity::Inclusive;
                enumerationKey.SequenceNumberToSkipForKeyLow = memoryTableValue.WriteSequenceNumber;
            }
            else
            {
                // The row resolved as Committed and the write sequence number is good.
                // Yield it to the caller.
                co_yield &memoryTableValue.Row;

                // Change the enumeration key to be exclusive so that we'll
                // skip all lower sequence numbers for the same row.
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

task<OperationOutcome> MemoryTable::ResolveMemoryTableRowOutcome(
    MemoryTableValue& memoryTableValue,
    MemoryTableOperationOutcomeTask task
)
{
    auto memoryTableOperationOutcome = co_await task;

    assert(memoryTableOperationOutcome.Outcome == OperationOutcome::Aborted
        || memoryTableOperationOutcome.Outcome == OperationOutcome::Committed);

    if (memoryTableOperationOutcome.Outcome == OperationOutcome::Committed)
    {
        // The completed operation must happen at a sequence number at least as
        // late as the current row, or snapshot isolation will have failed.
        assert(memoryTableOperationOutcome.WriteSequenceNumber >= memoryTableValue.WriteSequenceNumber);

        // The transaction committed, and we can safely write the 
        // completion information to the transaction.
        memoryTableValue.WriteSequenceNumber = memoryTableOperationOutcome.WriteSequenceNumber;
        
        memoryTableValue.OperationOutcome.store(
            OperationOutcome::Committed,
            std::memory_order_release
        );
    }
    else
    {
        // The transaction aborted.  Some other resolver may
        // have noticed and may have already written the outcome,
        // and yet another AddRow might have replaced the outcome,
        // so conditionally write the outcome if it is still Unknown.
        OperationOutcome expected = OperationOutcome::Unknown;

        memoryTableValue.OperationOutcome.compare_exchange_strong(
            expected,
            OperationOutcome::Aborted,
            std::memory_order_relaxed
        );
    }

    co_return memoryTableOperationOutcome.Outcome;
}

task<OperationOutcome> MemoryTable::ResolveMemoryTableRowOutcome(
    MemoryTableValue& memoryTableValue
)
{
    auto lock = co_await memoryTableValue.Mutex.scoped_lock_async();
    auto outcome = memoryTableValue.OperationOutcome.load(
        std::memory_order_acquire);

    if (outcome != OperationOutcome::Unknown)
    {
        co_return outcome;
    }

    co_return co_await ResolveMemoryTableRowOutcome(
        memoryTableValue,
        memoryTableValue.AsyncOperationOutcome);
}

task<> MemoryTable::Join()
{
    co_await m_asyncScope.join();
}

MemoryTable::MemoryTableValue::MemoryTableValue(
    InsertionKey&& other
)
    :
    Row { move(other.Row) },
    WriteSequenceNumber { other.Row.WriteSequenceNumber },
    AsyncOperationOutcome { move(other.AsyncOperationOutcome) },
    OperationOutcome { OperationOutcome::Unknown }
{
}

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

}