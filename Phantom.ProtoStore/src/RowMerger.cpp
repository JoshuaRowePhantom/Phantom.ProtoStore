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

RowMerger::row_generator RowMerger::Merge(
    row_generators rowSources
)
{
    std::vector<cppcoro::async_generator<const MemoryTableRow*>> capturedRowSources;
    for (auto& rowSource : rowSources)
    {
        capturedRowSources.emplace_back(
            move(rowSource));
    }

    auto comparator = [this](
        const MemoryTableRow* row1,
        const MemoryTableRow* row2
        )
    {
        return m_keyComparer->Compare(
            row1->Key.get(),
            row2->Key.get()
        ) == std::weak_ordering::less;
    };

    auto result = merge_sorted_generators<const MemoryTableRow*>(
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