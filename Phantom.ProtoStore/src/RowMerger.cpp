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
        return m_keyComparer->Compare(
            row1.Key,
            row2.Key
        ) == std::weak_ordering::less;
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

}