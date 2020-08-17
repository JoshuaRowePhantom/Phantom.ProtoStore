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

}
