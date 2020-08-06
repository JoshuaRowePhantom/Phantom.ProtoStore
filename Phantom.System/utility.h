#pragma once

#include <span>
#include <cppcoro/shared_task.hpp>
#include <cppcoro/sync_wait.hpp>
#include <cppcoro/task.hpp>

namespace Phantom
{
// A constant value false that depends on its name,
// for use in static_assert.
template<typename T>
constexpr bool always_false = false;

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
std::span<std::byte> as_bytes(
    T& value)
{
    return std::as_writable_bytes(std::span(
        &value,
        1));
}

template<typename T>
std::span<const std::byte> as_bytes(
    const T& value)
{
    return std::as_bytes(std::span(
        &value,
        1));
}

template<typename T>
std::unique_ptr<T> copy_unique(
    const T& other)
{
    return make_unique<T>(other);
}

struct empty
{};

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
