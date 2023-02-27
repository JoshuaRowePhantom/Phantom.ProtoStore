#include <exception>
#include <functional>
#include <queue>
#include <utility>
#include <cppcoro/async_generator.hpp>
#include <cppcoro/async_mutex.hpp>
#include <cppcoro/async_scope.hpp>
#include <cppcoro/async_auto_reset_event.hpp>
#include <cppcoro/task.hpp>
#include "async_utility.h"

namespace Phantom
{

template<
    typename TItem,
    typename TBeginIterator,
    typename TEndIterator,
    typename TComparer = std::less<TItem>
>
cppcoro::async_generator<TItem> merge_sorted_generators(
    TBeginIterator beginIterator,
    TEndIterator endIterator,
    TComparer comparer = TComparer()
)
{
    using generator_type = std::decay_t<decltype(*beginIterator)>;
    using iterator_type = Phantom::Coroutines::awaitable_result_type_t<decltype(std::declval<generator_type>().begin())>;

    struct entry
    {
        iterator_type* iterator;
        size_t generatorIndex;
    };

    auto entryComparer = [&](
        const entry& left,
        const entry& right
        )
    {
        // The comparer is a less-than comparer,
        // the lists are sorted smallest-to-highest,
        // so we want the highest item to be lower priority than the lowest item,
        // therefore compare opposite to the caller's comparer.
        return comparer(
            **right.iterator,
            **left.iterator
        );
    };

    std::priority_queue<entry, std::vector<entry>, decltype(entryComparer)> entries(
        entryComparer);

    std::vector<generator_type> generators;
    while (beginIterator != endIterator)
    {
        generators.emplace_back(std::move(*beginIterator));
        ++beginIterator;
    }

    std::vector<iterator_type> iterators;
    iterators.reserve(generators.size());

    for (size_t generatorIndex = 0; generatorIndex < generators.size(); generatorIndex++)
    {
        iterators.emplace_back(co_await generators[generatorIndex].begin());
        if (iterators[generatorIndex] != generators[generatorIndex].end())
        {
            entries.push(entry
                {
                    &iterators.back(),
                    generatorIndex
                });
        }
    }

    while(entries.size())
    {
        auto next = entries.top();
        entries.pop();

        co_yield **next.iterator;

        co_await++(*next.iterator);

        if (*next.iterator != generators[next.generatorIndex].end())
        {
            entries.push(next);
        }
    }
}

}