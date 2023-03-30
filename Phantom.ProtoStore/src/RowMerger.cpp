#include "RowMerger.h"
#include "Phantom.System/merge.h"
#include "ValueComparer.h"
#include "Schema.h"
#include <flatbuffers/flatbuffers.h>

namespace Phantom::ProtoStore
{

RowMerger::RowMerger(
    std::shared_ptr<const ValueComparer> keyComparer
)
    :
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
            && m_keyComparer->Equals(previousRow.Key, row.Key))
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

row_generator RowMerger::FilterTopLevelMergeSnapshotWindowRows(
    row_generator source,
    SequenceNumber earliestSequenceNumber
)
{
    ProtoValue previousKey;
    SequenceNumber previousSequenceNumber;

    for (auto iterator = co_await source.begin();
        iterator != source.end();
        co_await ++iterator)
    {
        ResultRow& currentRow = *iterator;

        // Skip rows for the same key IFF we've already enumerated
        // all rows up to the earliest sequence number for that key.
        if (previousKey
            &&
            m_keyComparer->Equals(previousKey, currentRow.Key)
            &&
            previousSequenceNumber <= earliestSequenceNumber)
        {
            continue;
        }

        previousKey = currentRow.Key;
        previousSequenceNumber = currentRow.WriteSequenceNumber;

        // If the row is outside the snapshot window and is a delete,
        // drop it.
        if (currentRow.WriteSequenceNumber < earliestSequenceNumber
            && !currentRow.Value)
        {
            continue;
        }

        co_yield std::move(*iterator);
    }
}

}