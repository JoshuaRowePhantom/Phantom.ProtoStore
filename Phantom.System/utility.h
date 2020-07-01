#pragma once

#include <cppcoro/sync_wait.hpp>

namespace Phantom::System
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
}