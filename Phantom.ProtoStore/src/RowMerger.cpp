#include "RowMerger.h"
#include "Phantom.System/merge.h"
#include "ValueComparer.h"
#include "Schema.h"
#include <flatbuffers/flatbuffers.h>

namespace Phantom::ProtoStore
{

RowMerger::RowMerger(
    std::shared_ptr<const Schema> schema,
    std::shared_ptr<const ValueComparer> keyComparer
)
    :
    m_schema(std::move(schema)),
    m_keyComparer(std::move(keyComparer))
{}

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
            row1.Key,
            row2.Key
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
        ResultRow& row = *iterator;

        if (previousRow.Key
            && m_keyComparer->Compare(previousRow.Key, row.Key) == std::weak_ordering::equivalent)
        {
            continue;
        }
        
        previousRow = row;

        if (!row.Value)
        {
            continue;
        }

        co_yield std::move(row);
    }
}
}