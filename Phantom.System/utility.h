#pragma once

#include <concepts>
#include <functional>
#include <memory>
#include <span>
#include "concepts.h"

namespace Phantom
{
// A constant value false that depends on its name,
// for use in static_assert.
template<typename T>
constexpr bool always_false = false;

struct empty
{};

template<typename T>
    requires !is_span<T>
std::span<std::byte> as_bytes(
    T& value)
{
    return std::as_writable_bytes(std::span(
        &value,
        1));
}

template<typename T>
    requires !is_span<T>
std::span<const std::byte> as_bytes(
    const T& value)
{
    return std::as_bytes(std::span(
        &value,
        1));
}

template<typename T>
auto copy_unique(
    T&& other)
{
    return std::make_unique<std::decay_t<T>>(other);
}

template<typename T>
auto copy_shared(
    T&& other)
{
    return std::make_shared<std::decay_t<T>>(other);
}

template<
    typename T,
    typename Lambda = std::function<T()>
>
class value_factory
{
    Lambda m_lambda;

public:
    template<
        std::convertible_to<T> Other,
        std::convertible_to<Lambda> OtherLambda
    >
    value_factory(
        value_factory<Other, OtherLambda>&& other
    ) :
        m_lambda(std::move(other.m_lambda))
    {}

    template<
        std::convertible_to<T> Other,
        std::convertible_to<Lambda> OtherLambda
    >
    value_factory(
        const value_factory<Other, OtherLambda>& other
    ) :
        m_lambda(other.m_lambda)
    {}

    template<
        std::convertible_to<T> Other,
        std::convertible_to<Lambda> OtherLambda
    >
    value_factory& operator =(
        value_factory<Other, OtherLambda>&& other
    )
    {
        m_lambda = std::move(other.m_lambda);
        return *this;
    }

    template<
        std::convertible_to<T> Other,
        std::convertible_to<Lambda> OtherLambda
    >
    value_factory& operator=(
        const value_factory<Other, OtherLambda>& other
    )
    {
        m_lambda = other.m_lambda;
        return *this;
    }

    value_factory(
        std::convertible_to<Lambda> auto lambda
    ) 
        noexcept(std::is_nothrow_constructible_v<Lambda, decltype(lambda)>)
        : m_lambda{ std::forward<decltype(lambda)>(lambda) }
    {}

    auto operator()() const noexcept(noexcept(m_lambda()))
    {
        return m_lambda();
    }

    operator T() const noexcept(noexcept(m_lambda()))
    {
        return m_lambda();
    }

    template<
        typename Value
    >
    requires std::convertible_to<T, Value>
    operator Value() const noexcept(noexcept(Value(m_lambda())))
    {
        return Value(m_lambda());
    }
};

template<
    std::invocable Lambda
> value_factory(Lambda) -> value_factory<std::invoke_result_t<std::decay_t<Lambda>>, std::decay_t<Lambda>>;

template<typename T, typename... Args>
auto make_forwarding_value_factory(
    Args&&... args
)
{
    return value_factory{[&] { return T{ std::forward<Args>(args)... }; }};
}

// Make an std::shared_ptr to a value that is constructed in-place,
// and maintain a strong reference to another object as part of
// the resulting shared_ptr.
//
// This is useful when the arguments' lifetimes are controlled by
// the strongReference, and the argument lifetimes should exceed
// the lifetime of the constructed object.
template<
    typename T, 
    typename Reference,
    typename ... Args>
std::shared_ptr<T> make_shared_with_strong_reference(
    Reference&& strongReference,
    Args&&... args
)
{
    using value_type = std::decay_t<T>;
    using strong_reference_type = std::remove_reference_t<Reference>;

    struct HolderType
    {
        strong_reference_type m_strongReference;
        value_type m_value;

        HolderType(
            Reference&& strongReference,
            auto&& value
        ) :
            m_strongReference{ std::forward<Reference>(strongReference) },
            m_value { std::forward<decltype(value)>(value) }
        {}
    };

    auto holder = std::make_shared<HolderType>(
        std::forward<Reference>(strongReference),
        make_forwarding_value_factory<value_type>(std::forward<decltype(args)>(args)...)
    );

    return std::shared_ptr<T>
    {
        std::move(holder),
        & holder->m_value,
    };
}

}
