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
            ownedRow.get()
        };

        MemoryTableValue memoryTableValue
        {
            ownedRow.get(),
            nullptr,
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
    }
}

cppcoro::async_generator<const MemoryTableRow*> MemoryTable::Enumerate(
    SequenceNumber transactionReadSequenceNumber,
    SequenceNumber requestedReadSequenceNumber,
    KeyRangeEnd low,
    KeyRangeEnd high
)
{
    EnumerationKey enumerationKey
    {
        low.Value,
        low.Inclusivity,
        transactionReadSequenceNumber,
        requestedReadSequenceNumber,
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

        co_yield row;

        enumerationKey.KeyLow = row->Key.get();
        enumerationKey.Inclusivity = Inclusivity::Exclusive;

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

    if (key1->SequenceNumber >= key2.ReadSequenceNumber
        ||
        key1->SequenceNumber >= key2.Row->SequenceNumber)
    {
        // By returning equivalent here, the SkipList
        // won't insert the row.
        return std::weak_ordering::equivalent;
    }

    return std::weak_ordering::greater;
}

}