#pragma once

#include <experimental/resumable>
#include <functional>
#include <type_traits>
#include <cppcoro/shared_task.hpp>
#include <cppcoro/sync_wait.hpp>
#include <cppcoro/task.hpp>

namespace Phantom
{

template<typename T>
concept can_co_await = requires(T t)
{
    { t.operator co_await() };
};

template<typename T>
inline constexpr bool can_co_await_v = false;

template<can_co_await T>
inline constexpr bool can_co_await_v<T> = true;

template<
    typename TCallable,
    typename ... TArgs>
    inline constexpr bool has_co_awaitable_result_v = can_co_await_v<std::invoke_result<TCallable, TArgs...>::type>;

template<
    can_co_await T
> T as_awaitable(
    T&& t
)
{
    return t;
}

template<
    typename T
> class simple_awaitable
    : public std::experimental::suspend_never
{
    T m_t;
public:
    simple_awaitable(
        T&& t
    ) : m_t(std::move(t))
    {}

    simple_awaitable(
        const T& t
    ) : m_t(t)
    {}

    T& await_resume() & noexcept
    {
        return m_t;
    }

    T&& await_resume() && noexcept
    {
        return std::move(m_t);
    }
};

template<
    typename T
> simple_awaitable<std::remove_reference_t<T>> as_awaitable(
    T&& t
)
{
    return simple_awaitable(std::forward<T>(t));
}

template<
    typename TCallable,
    typename... TArgs
> auto invoke_as_awaitable(
    TCallable callable,
    TArgs&&...  args
)
{
    return as_awaitable(
        std::invoke(
            callable,
            std::forward<TArgs>(args)...)
    );
}

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

// Given two ordinary iterators, a key, and a possibly comparer that returns
// true if *iterator < value, return the highest iterator not greater
// than the value.
// Any of these may be async:
//   comparer.operator()
//   TIterator::operator++()
//   TIterator::operator+=()
template <
    typename TIterator,
    typename TKey,
    typename TComparer
> TIterator async_lower_bound(
    TIterator first,
    TIterator last,
    const TKey& value,
    TComparer comparer
)
{
    auto count = co_await as_awaitable(
        first - last);

    while (count > 0) 
    {
        auto middle = first;
        difference_type step = count / 2;

        co_await as_awaitable(
            middle += step);

        auto isLessThan = co_await as_awaitable(lessThanComparer(
            *middle,
            value));

        if (isLessThan)
        {
            first = co_await as_awaitable(++middle);
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
// Any of these may be async:
//   comparer.operator()
//   TIterator::operator++()
//   TIterator::operator+=()
//   TIterator::operator-()
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
    auto count = co_await as_awaitable(
        first - last);

    while (count > 0)
    {
        auto middle = first;
        auto step = count / 2;

        co_await as_awaitable(
            middle += step);

        auto isLessThan = co_await as_awaitable(lessThanComparer(
            value,
            *middle));

        if (!isLessThan)
        {
            first = co_await as_awaitable(
                ++middle);
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
