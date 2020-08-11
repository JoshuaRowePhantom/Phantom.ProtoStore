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
    std::vector<cppcoro::async_generator<ResultRow>> capturedRowSources;
    for (auto& rowSource : rowSources)
    {
        capturedRowSources.emplace_back(
            move(rowSource));
    }

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
        capturedRowSources.begin(),
        capturedRowSources.end(),
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

    optional<string> previousKey;
    string currentKey;

    for (auto iterator = co_await mergeEnumeration.begin();
        iterator != mergeEnumeration.end();
        co_await ++iterator)
    {
        (*iterator).Key->SerializeToString(
            &currentKey
        );

        if (previousKey == currentKey)
        {
            continue;
        }

        previousKey = move(currentKey);

        if (!(*iterator).Value)
        {
            continue;
        }

        co_yield *iterator;
    }
}
}