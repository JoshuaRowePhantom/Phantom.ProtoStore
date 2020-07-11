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
    OperationOutcomeTask operationOutcome
)
{
    InsertionKey insertionKey
    {
        .Key = row.Key.get(),
        .WriteSequenceNumber = row.WriteSequenceNumber,
        .OperationOutcome = operationOutcome,
        .ReadSequenceNumber = readSequenceNumber,
    };

    auto [iterator, succeeded] = m_skipList.insert(
        insertionKey);

    if (!succeeded)
    {
        throw WriteConflict();
    }

    iterator->Row = move(row);

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
        low.Key,
        low.Inclusivity,
        readSequenceNumber,
    };

    auto [findIterator, keyComparisonResult] = m_skipList.find(
        enumerationKey);

    while (findIterator)
    {
        const MemoryTableValue& value = *findIterator;

        auto highComparisonResult = m_comparer(
            value,
            high);

        if (highComparisonResult == std::weak_ordering::greater)
        {
            co_return;
        }

        enumerationKey.KeyLow = value.Key;

        if (value.Row.WriteSequenceNumber > readSequenceNumber)
        {
            // We found a version of the row later than requested;
            // change the enumeration key to be inclusive so that we'll
            // locate the highest sequence number for the row that
            // is lower than the requested sequence number.
            enumerationKey.Inclusivity = Inclusivity::Inclusive;
        }
        else
        {
            // We found a version of the row to return.
            // Change the enumeration key to be exclusive so that we'll
            // skip all lower sequence numbers for the same row.
            enumerationKey.Inclusivity = Inclusivity::Exclusive;
            co_yield &value.Row;
        }

        keyComparisonResult = m_skipList.find_in_place(
            enumerationKey,
            findIterator
        );
    }
}

MemoryTable::InsertionKey::operator MemoryTable::MemoryTableValue() const
{
    MemoryTableRow row =
    {
        .Key = nullptr,
        .WriteSequenceNumber = WriteSequenceNumber,
        .Value = nullptr,
    };

    return
    {
        .Key = Key,
        .Row = move(row),
        .OperationOutcome = move(OperationOutcome),
    };
}

std::weak_ordering MemoryTable::MemoryTableRowComparer::operator()(
    const MemoryTableValue& key1,
    const InsertionKey& key2
    ) const
{
    auto comparisonResult = (*m_keyComparer)(
        key1.Key,
        key2.Key);

    if (comparisonResult != std::weak_ordering::equivalent)
    {
        return comparisonResult;
    }

    auto sequenceNumber = key1.Row.WriteSequenceNumber;

    if (sequenceNumber > key2.ReadSequenceNumber
        ||
        sequenceNumber >= key2.WriteSequenceNumber)
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
            key1.Key,
            key2.KeyLow);

    if (comparisonResult == std::weak_ordering::equivalent
        &&
        key2.Inclusivity == Inclusivity::Exclusive)
    {
        return std::weak_ordering::less;
    }

    if (comparisonResult != std::weak_ordering::equivalent)
    {
        return comparisonResult;
    }

    auto sequenceNumber = key1.Row.WriteSequenceNumber;

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
            key1.Key,
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