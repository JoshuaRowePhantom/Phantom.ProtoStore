#include "RowMerger.h"
#include "Phantom.System/merge.h"
#include "KeyComparer.h"

namespace Phantom::ProtoStore
{

RowMerger::RowMerger(
    KeyComparer* keyComparer
) :
    m_keyComparer(keyComparer)
{
}

row_generator RowMerger::Merge(
    row_generators rowSources
)
{
    auto comparator = [this](
        const ResultRow& row1,
        const ResultRow& row2
        )
    {
        auto keyOrdering = m_keyComparer->Compare(
            *row1.Key,
            *row2.Key
        );

        if (keyOrdering == std::weak_ordering::less)
        {
            return true;
        }

        if (keyOrdering == std::weak_ordering::greater)
        {
            return false;
        }

        return row1.WriteSequenceNumber > row2.WriteSequenceNumber;
    };

    auto result = merge_sorted_generators<ResultRow>(
        rowSources.begin(),
        rowSources.end(),
        comparator);

    for (auto iterator = co_await result.begin();
        iterator != result.end();
        co_await ++iterator)
    {
        co_yield *iterator;
    }
}

row_generator RowMerger::Enumerate(
    row_generators rowSources
)
{
    auto mergeEnumeration = Merge(
        move(rowSources));

    ResultRow previousRow;

    for (auto iterator = co_await mergeEnumeration.begin();
        iterator != mergeEnumeration.end();
        co_await ++iterator)
    {
        auto& row = *iterator;

        if (previousRow.Key->data()
            && std::ranges::equal(*previousRow.Key, *row.Key))
        {
            continue;
        }
        
        previousRow = row;

        if (!row.Value->data())
        {
            continue;
        }

        co_yield std::move(row);
    }
}
}