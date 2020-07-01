#pragma once

#include <span>
#include <cppcoro/sync_wait.hpp>

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
}