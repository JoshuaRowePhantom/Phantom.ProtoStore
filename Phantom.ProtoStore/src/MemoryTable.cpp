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
    MemoryTableRow& row
)
{
    auto ownedRow = make_unique<MemoryTableRow>(
        move(row));

    try
    {
        InsertionKey insertionKey
        {
            .Row = ownedRow.get(),
            .ReadSequenceNumber = readSequenceNumber,
        };

        MemoryTableValue memoryTableValue
        {
            .RawRow = ownedRow.get(),
            .OwnedRow = nullptr,
        };

        auto [iterator, succeeded] = m_skipList.insert(
            insertionKey,
            std::move(memoryTableValue));

        if (!succeeded)
        {
            throw WriteConflict();
        }

        iterator->second.OwnedRow = move(ownedRow);

        co_return;
    }
    catch (...)
    {
        row = move(
            *ownedRow);

        throw;
    }
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
        const MemoryTableRow* row = findIterator->second.Row();

        auto highComparisonResult = m_comparer(
            row,
            high);

        if (highComparisonResult == std::weak_ordering::greater)
        {
            co_return;
        }

        if (row->TransactionId)
        {
            throw UnresolvedTransactionConflict();
        }

        enumerationKey.KeyLow = row->Key.get();

        if (row->SequenceNumber > readSequenceNumber)
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
            co_yield row;
        }

        keyComparisonResult = m_skipList.find_in_place(
            enumerationKey,
            findIterator
        );
    }
}


std::weak_ordering MemoryTable::MemoryTableRowComparer::operator()(
    const MemoryTableRow* key1,
    const InsertionKey& key2
    ) const
{
    auto comparisonResult = (*m_keyComparer)(
        key1->Key.get(),
        key2.Row->Key.get());

    if (comparisonResult != std::weak_ordering::equivalent)
    {
        return comparisonResult;
    }

    if (key1->SequenceNumber > key2.ReadSequenceNumber
        ||
        key1->SequenceNumber >= key2.Row->SequenceNumber)
    {
        // By returning equivalent here, the SkipList
        // won't insert the row.
        return std::weak_ordering::equivalent;
    }

    return std::weak_ordering::greater;
}

std::weak_ordering MemoryTable::MemoryTableRowComparer::operator()(
    const MemoryTableRow* key1,
    const EnumerationKey& key2
    ) const
{
    auto comparisonResult = 
        !key2.KeyLow
        ?
        std::weak_ordering::greater
        :
        (*m_keyComparer)(
            key1->Key.get(),
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

    if (key1->SequenceNumber > key2.ReadSequenceNumber)
    {
        return std::weak_ordering::less;
    }

    if (key1->SequenceNumber == key2.ReadSequenceNumber)
    {
        return std::weak_ordering::equivalent;
    }

    return std::weak_ordering::greater;
}

std::weak_ordering MemoryTable::MemoryTableRowComparer::operator()(
    const MemoryTableRow* key1,
    const KeyRangeEnd& key2
    ) const
{
    auto comparisonResult =
        !key2.Key
        ?
        std::weak_ordering::less
        :
        (*m_keyComparer)(
            key1->Key.get(),
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