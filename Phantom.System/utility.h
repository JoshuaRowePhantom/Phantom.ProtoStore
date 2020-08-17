#pragma once

#include <memory>
#include <span>

namespace Phantom
{
// A constant value false that depends on its name,
// for use in static_assert.
template<typename T>
constexpr bool always_false = false;

struct empty
{};

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

}
