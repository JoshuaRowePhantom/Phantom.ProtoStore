#pragma once

#include <coroutine>
#include <functional>
#include <type_traits>
#include <cppcoro/awaitable_traits.hpp>
#include <cppcoro/is_awaitable.hpp>
#include <cppcoro/shared_task.hpp>
#include <cppcoro/sync_wait.hpp>
#include <cppcoro/task.hpp>

namespace Phantom
{

template<typename T>
concept can_co_await = cppcoro::is_awaitable_v<T>;

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
> decltype(auto) as_awaitable(
    T&& t
)
{
    return std::move(t);
}

template<
    typename T
> class [[nodiscard]] simple_awaitable
    : public std::suspend_never
{
    T&& m_t;
public:
    simple_awaitable(
        T&& t
    ) : m_t(std::forward<T>(t))
    {}

    T&& await_resume() noexcept
    {
        return std::forward<T>(m_t);
    }

    simple_awaitable& operator co_await()&&
    {
        return *this;
    }
};

template<
    typename T
> simple_awaitable<T> as_awaitable(
    T&& t
)
{
    return simple_awaitable<T>(std::forward<T>(t));
}

template<
    typename From,
    typename To
> concept as_awaitable_convertible_to
= requires(From value)
{
    { cppcoro::sync_wait(as_awaitable(value)) } -> std::convertible_to<To>;
};

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

inline cppcoro::task<> make_completed_task()
{
    co_return;
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
//   TIterator::operator-()
//   TIterator::operator*()
//   decltype(TIterator::operator*())
// Implementation adapter from https://en.cppreference.com/w/cpp/algorithm/lower_bound
template <
    typename TIterator,
    typename TKey,
    typename TComparer
> cppcoro::task<TIterator> async_lower_bound(
    TIterator first,
    TIterator last,
    const TKey& value,
    TComparer lessThanComparer
)
{
    auto count = co_await as_awaitable(
        last - first);

    while (count > 0) 
    {
        auto iterator = first;
        auto step = count / 2;

        co_await as_awaitable(
            iterator += step);

        auto isLessThan = co_await as_awaitable(lessThanComparer(
            co_await as_awaitable(co_await as_awaitable(*iterator)),
            value
        ));

        if (isLessThan)
        {
            first = co_await as_awaitable(++iterator);
            count -= step + 1;
        }
        else
        {
            count = step;
        }
    }

    co_return first;
}

// Given two ordinary iterators, a key, and a co_awaitable comparer that returns
// true if value < *iterator, return the lowest iterator not less
// than the value.
// Any of these may be async:
//   comparer.operator()
//   TIterator::operator++()
//   TIterator::operator+=()
//   TIterator::operator-()
//   TIterator::operator*()
//   decltype(TIterator::operator*())
template <
    typename TIterator,
    typename TKey,
    typename TComparer
> cppcoro::task<TIterator> async_upper_bound(
    TIterator first,
    TIterator last,
    const TKey& value,
    TComparer lessThanComparer
)
{
    auto count = co_await as_awaitable(
        last - first);

    while (count > 0)
    {
        auto iterator = first;
        auto step = count / 2;

        co_await as_awaitable(
            iterator += step);

        auto isLessThan = co_await as_awaitable(lessThanComparer(
            value,
            co_await as_awaitable(co_await as_awaitable(*iterator))
        ));

        if (!isLessThan)
        {
            first = co_await as_awaitable(
                ++iterator);
            count -= step + 1;
        }
        else
        {
            count = step;
        }
    }

    co_return first;
}

// This method exists mostly to be able to access the return type of
// awaiting a potentially-awaitable value in concept definitions, but in theory
// it could be used to access the result of a potentially-awaitable type
// by awaiting it on the current thread.
template<
    typename TAwaitable
> auto as_sync_awaitable(
    TAwaitable&& awaitable
)
{
    return cppcoro::sync_wait(as_awaitable(
        std::forward<TAwaitable>(awaitable)));
}

template<
    typename TCollection
> struct as_awaitable_async_enumerable_traits
{
    typedef decltype(as_awaitable(std::declval<TCollection>().begin())) begin_awaitable_type;
    typedef typename cppcoro::awaitable_traits<begin_awaitable_type>::await_result_type iterator_type;
    typedef decltype((*declval<iterator_type>())) value_type;
};

template<
    typename TAsyncIterator
> concept as_awaitable_iterator
= requires(
    TAsyncIterator iterator
    )
{
    { cppcoro::sync_wait(as_awaitable(++iterator)) };
    { iterator == iterator } -> std::convertible_to<bool>;
    { iterator != iterator } -> std::convertible_to<bool>;
};

template<
    typename TCollection
> concept as_awaitable_async_enumerable =
requires(
    TCollection collection
    )
{
    { as_sync_awaitable(collection.begin()) } -> as_awaitable_iterator;
    { collection.end() };
    //{ as_sync_awaitable(collection.begin()) != collection.end() } -> std::convertible_to<bool>;
    //{ as_sync_awaitable(collection.begin()) == collection.end() } -> std::convertible_to<bool>;
};

template<
    typename TAsyncIterator,
    typename TValue
> concept as_awaitable_iterator_of
=
as_awaitable_iterator<TAsyncIterator>
&&
requires(
    TAsyncIterator iterator)
{
    { *iterator } -> std::convertible_to<TValue>;
};


template<
    typename TCollection,
    typename TValue
> concept as_awaitable_async_enumerable_of
=
as_awaitable_async_enumerable<TCollection>
&&
requires(
    TCollection collection
    )
{
    { as_sync_awaitable(collection.begin()) } -> as_awaitable_iterator_of<TValue>;
};

template<
    as_awaitable_iterator TIterator
> cppcoro::task<> async_skip(
    TIterator begin,
    TIterator end
)
{
    while (begin != end)
    {
        co_await as_awaitable(++begin);
    }
}

template<
    typename TEvent
> concept Event
= can_co_await<TEvent>
&& requires(
    TEvent event
    )
{
    { event.set() };
};

}
