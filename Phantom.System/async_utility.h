#pragma once

#include <experimental/resumable>
#include <functional>
#include <type_traits>
#include <cppcoro/shared_task.hpp>
#include <cppcoro/sync_wait.hpp>
#include <cppcoro/task.hpp>

namespace Phantom
{

template<
    typename TFunctor
>
auto run_async(
    TFunctor functor)
{
    return cppcoro::sync_wait(
        functor());
}

template<typename T>
cppcoro::task<T> make_completed_task(T&& value)
{
    co_return std::forward<T>(value);
}

inline cppcoro::task<> make_completed_task()
{
    co_return;
}

template<typename T>
cppcoro::shared_task<T> make_completed_shared_task(T&& value)
{
    co_return std::forward<T>(value);
}

inline cppcoro::shared_task<> make_completed_shared_task()
{
    co_return;
}

// Given two ordinary iterators, a key, and a co_awaitable comparer that returns
// true if *iterator < value, return the highest iterator not greater
// than the value.
template <
    typename TIterator,
    typename TKey,
    typename TComparer
> TIterator async_lower_bound(
    TIterator first,
    TIterator last,
    const TKey& value,
    TComparer asyncLessThanComparer
)
{
    typedef typename std::iterator_traits<TIterator>::difference_type difference_type;
    
    difference_type count = std::distance(
        first,
        last);

    while (count > 0) 
    {
        auto middle = first;
        difference_type step = count / 2;
        std::advance(
            middle, 
            step);

        auto isLessThan = co_await lessThanComparer(
            *middle,
            value);

        if (isLessThan)
        {
            first = ++middle;
            count -= step + 1;
        }
        else
        {
            count = step;
        }
    }

    return first;
}

// Given two ordinary iterators, a key, and a co_awaitable comparer that returns
// true if value < *iterator, return the lowest iterator not less
// than the value.
template <
    typename TIterator,
    typename TKey,
    typename TComparer
> TIterator async_upper_bound(
    TIterator first,
    TIterator last,
    const TKey& value,
    TComparer asyncLessThanComparer
)
{
    typedef typename std::iterator_traits<TIterator>::difference_type difference_type;

    difference_type count = std::distance(
        first,
        last);

    while (count > 0)
    {
        auto middle = first;
        difference_type step = count / 2;

        std::advance(
            middle,
            step);

        auto isLessThan = co_await lessThanComparer(
            value,
            *middle);

        if (!isLessThan)
        {
            first = ++middle;
            count -= step + 1;
        }
        else
        {
            count = step;
        }
    }

    return first;
}

}
