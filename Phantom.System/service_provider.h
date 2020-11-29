#pragma once

#include<tuple>
#include<type_traits>

namespace Phantom
{
namespace detail
{
// Determines if two services are compatible to be in a service list, i.e. they are not convertible to each other.
template <
    typename TService,
    typename TOtherService
>
constexpr bool ServicesAreCompatible
=
!std::is_convertible_v<TService, TOtherService>
&& !std::is_convertible_v<TOtherService, TService>;

// Determine if a service is compatible with a list of other services.
template<
    typename TService,
    typename ... TOtherServices
>
constexpr bool ServiceIsCompatibleWithOtherServices
=
(... && ServicesAreCompatible<TService, TOtherServices>);


// Determine if a list of services is compatible.
// A list of zero services is compatible.
template<
    typename... TServices
>
constexpr bool ServiceListIsCompatible =
true;

// A list of one or more services is compatible
// if the first service is compatible with the rest of the list,
// and the rest of the list is a compatible list.
template<
    typename TService,
    typename... TOtherServices
>
constexpr bool ServiceListIsCompatible<TService, TOtherServices...>
=
ServiceIsCompatibleWithOtherServices<TService, TOtherServices...>
&& ServiceListIsCompatible<TOtherServices...>;

// Now define the CompatibleServiceList concept.
template<
    typename... TServices
> concept CompatibleServiceList =
ServiceListIsCompatible<TServices...>;


template<
    typename ... TServices
>
requires ServiceListIsCompatible<TServices...>
struct service_list_type
{
    typedef std::tuple<TServices...> tuple_type;
};

template<
    typename... TOverrideServices,
    size_t... CompatibleIndices,
    size_t IndexToCheck
> constexpr auto GetCompatibleIndicesForOverrides(
    service_list_type<> originalServices,
    service_list_type<TOverrideServices...> overrideServices,
    std::integer_sequence<size_t, CompatibleIndices...> validOriginalServiceIndices,
    std::integral_constant<size_t, IndexToCheck> indexToCheck
)
{
    return validOriginalServiceIndices;
}

template<
    typename TOriginalService,
    typename... TOriginalServices,
    typename... TOverrideServices,
    size_t... CompatibleIndices,
    size_t IndexToCheck
> constexpr auto GetCompatibleIndicesForOverrides(
    service_list_type<TOriginalService, TOriginalServices...> originalServices,
    service_list_type<TOverrideServices...> overrideServices,
    std::integer_sequence<size_t, CompatibleIndices...> validOriginalServiceIndices,
    std::integral_constant<size_t, IndexToCheck> indexToCheck
)
{
    if constexpr (ServiceIsCompatibleWithOtherServices<TOriginalService, TOverrideServices...>)
    {
        return GetCompatibleIndicesForOverrides(
            service_list_type<TOriginalServices...>(),
            overrideServices,
            std::integer_sequence<size_t, CompatibleIndices..., IndexToCheck>(),
            std::integral_constant<size_t, IndexToCheck + 1>()
        );
    }
    else
    {
        return GetCompatibleIndicesForOverrides(
            service_list_type<TOriginalServices...>(),
            overrideServices,
            std::integer_sequence<size_t, CompatibleIndices...>(),
            std::integral_constant<size_t, IndexToCheck + 1>()
        );
    }
}

template<
    typename TOriginalServiceList,
    typename TOverrideServiceList
> constexpr auto GetCompatibleIndicesForOverrides(
    TOriginalServiceList originalServices,
    TOverrideServiceList overrideServices
)
{
    return GetCompatibleIndicesForOverrides(
        originalServices,
        overrideServices,
        std::integer_sequence<size_t>(),
        std::integral_constant<size_t, 0>()
    );
}

template<
    typename TService,
    typename... TServices
> constexpr bool has_exact_service
=
(... || std::is_same_v<TService, TServices>);

}

template<
    typename ... TServices
> 
requires detail::ServiceListIsCompatible<TServices...>
class service_provider
{
public:
    typedef std::tuple<TServices...> tuple_type;

private:
    tuple_type m_tuple;

    template<
        size_t... OriginalServiceIndices,
        typename... TNewServices
    > auto override_services(
        const service_provider<TNewServices...>& serviceProvider,
        std::integer_sequence<size_t, OriginalServiceIndices...> indices
        ) const
    {
        return service_provider<
            std::tuple_element_t<OriginalServiceIndices, tuple_type>...,
            TNewServices...
        >(
            std::get<OriginalServiceIndices>(m_tuple)...,
            serviceProvider.get<TNewServices>()...
        );
    }

public:
    service_provider(
        TServices ... services
    ) : m_tuple(
        std::move(services)...)
    {}

    // This constructor converts a const reference of an arbitrary service provider by copying each service in this type.
    template<
        typename ... TOtherServices
    > service_provider(
        const service_provider<TOtherServices...>& other
    ) : m_tuple(
        other.get<TServices>()...
    ) {}

    // This constructor converts an r-value reference of an arbitrary service provider by forwarding each service in this type.
    template<
        typename ... TOtherServices
    > service_provider(
        service_provider<TOtherServices...>&& other
    ) : m_tuple(
        std::forward<TServices>(get<TServices>(other.m_tuple))...
    ) {}

    template<
        typename TService,
        typename Enabled = std::enable_if_t<detail::has_exact_service<TService, TServices...>>
    > const auto& get() const
    {
        return std::get<TService>(
            m_tuple);
    }

    // Get a new service provider with all the specified additional services. 
    // Produces a compiler error if there are duplicates.
    // this = lvalue
    // other = lvalue
    template<
        typename ... TNewServices
    >  service_provider<TServices..., TNewServices...> operator+(
        const service_provider<TNewServices...>& other
        ) const&
    {
        return service_provider<TServices..., TNewServices...>(
            get<TServices>()...,
            other.get<TNewServices>()...
            );
    }

    // Get a new service provider with all the specified additional services, overriding and dropping
    // any duplicate services.
    // this = lvalue
    // other = lvalue
    template<
        typename ... TNewServices
    >  auto operator/(
        const service_provider<TNewServices...>& other
        ) const
    {
        return override_services(
            other,
            detail::GetCompatibleIndicesForOverrides(
                detail::service_list_type<TServices...>(),
                detail::service_list_type<TNewServices...>()
                ));
    }
};

template<
    typename TService,
    typename ... TServices,
    typename enabled = std::void_t<
        decltype(
            std::declval<const service_provider<TServices...>&>()
            .get<TService>()
            )
    >
> auto get(
    const service_provider<TServices...>& serviceProvider)
{
    return serviceProvider.get<TService>();
}

}
