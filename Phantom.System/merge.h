#include <functional>
#include <queue>
#include <utility>
#include <cppcoro/generator.hpp>
#include <cppcoro/async_generator.hpp>

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
    typedef decltype(beginIterator->begin().await_resume()) iterator_type;
    typedef decltype(beginIterator->end()) end_iterator_type;

    std::vector<iterator_type> iterators;
    std::vector<end_iterator_type> endIterators;

    for (auto generatorIterator = beginIterator; generatorIterator != endIterator; generatorIterator++)
    {
        iterators.push_back(
            co_await generatorIterator->begin());
        endIterators.push_back(
            generatorIterator->end());
    }

    auto iteratorComparer = [&comparer, &iterators](
        size_t i1, 
        size_t i2)
    {
        return comparer(
            *iterators[i2],
            *iterators[i1]);
    };

    typedef std::priority_queue<size_t, std::vector<size_t>, decltype(iteratorComparer)> priorityQueueType;

    priorityQueueType priorityQueue(
        iteratorComparer);

    for (int iteratorIndex = 0; iteratorIndex < iterators.size(); iteratorIndex++)
    {
        if (iterators[iteratorIndex] != endIterators[iteratorIndex])
        {
            priorityQueue.push(
                iteratorIndex);
        }
    }

    while (!priorityQueue.empty())
    {
        auto iteratorIndex = priorityQueue.top();
        co_yield *iterators[iteratorIndex];
        priorityQueue.pop();

        if ((co_await ++iterators[iteratorIndex]) != endIterators[iteratorIndex])
        {
            priorityQueue.push(iteratorIndex);
        }
    }
}

}