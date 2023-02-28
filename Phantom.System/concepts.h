#pragma once

#include <type_traits>
#include <span>

namespace Phantom
{


// Determine if a given type is an instantiation of a template
// accepting type arguments.
namespace detail
{
template<
    typename T,
    template <typename ...> typename Template
> constexpr bool is_template_instantiation_v = false;

template<
    typename... Args,
    template <typename ...> typename Template
> constexpr bool is_template_instantiation_v<
    Template<Args...>,
    Template
> = true;
}

template<
    typename T,
    template <typename ...> typename Template
> concept is_template_instantiation = detail::is_template_instantiation_v<std::remove_cvref_t<T>, Template>;

namespace detail
{
template<
    typename T
> constexpr bool is_span_v = false;

template<
    typename T, size_t extent
> constexpr bool is_span_v<std::span<T, extent>> = true;
}

template<
    typename T
> concept is_span = detail::is_span_v<std::remove_cvref_t<T>>;

}
