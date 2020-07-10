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

task<> MemoryTable::AddRow(
    SequenceNumber readSequenceNumber,
    MemoryTableRow row
)
{
    throw 0;
    InsertionKey insertionKey
    { 
        &row 
    };

    MemoryTableValue memoryTableValue
    {
        &row,
        nullptr,
    };

    auto [iterator, succeeded] = m_skipList.insert(
        insertionKey,
        std::move(memoryTableValue));

    if (!succeeded)
    {
        throw WriteConflict();
    }

    co_return;
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

        enumerationKey.KeyLow = row->Key;
        enumerationKey.Inclusivity = Inclusivity::Exclusive;

        keyComparisonResult = m_skipList.find_in_place(
            enumerationKey,
            findIterator
        );
    }
}

}