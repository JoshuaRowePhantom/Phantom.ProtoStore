#include "RowMerger.h"
#include <cppcoro/generator.hpp>
#include <cppcoro/async_generator.hpp>
#include <cppcoro/fmap.hpp>
#include <cppcoro/when_all.hpp>
#include <queue>
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

cppcoro::async_generator<const MemoryTableRow*> RowMerger::Merge(
    cppcoro::generator<cppcoro::async_generator<const MemoryTableRow*>> rowSources
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