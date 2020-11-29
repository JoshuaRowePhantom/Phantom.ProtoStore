#include "StandardIncludes.h"
#include "Phantom.System/service_provider.h"

namespace Phantom
{
struct service_provider_tests
    : public testing::Test
{
    template<
        int tag
    > struct BaseService
    {
    };
    
    template<
        int tag
    > struct DerivedService
        : 
    public BaseService<tag>
    {
    };

    template<
        int 
    > struct IndependentService
    {};

    template<
        typename TService,
        typename TServiceProvider,
        typename = void
    > static constexpr bool has_service 
        = false;

    template<
        typename TService,
        typename TServiceProvider
    > static constexpr bool has_service<
        TService,
        TServiceProvider,
        std::void_t<decltype(get<TService>(std::declval<TServiceProvider>()))>
    > 
        = true;

    template<
        typename TService,
        typename TServiceProvider
    > void assert_has_service(
        TService expectedService,
        TServiceProvider serviceProvider
        )
    {
        ASSERT_EQ(
            get<TService>(serviceProvider),
            expectedService);
    }

    template<
        typename TService,
        typename TServiceProvider
    > void assert_doesnt_have_service(
        const TServiceProvider& serviceProvider
    )
    {
        static_assert(!has_service<TService, TServiceProvider>, "Service provider has an incorrect service.");
    }

    template<
        typename TServiceProvider1,
        typename TServiceProvider2,
        typename = void
    > static constexpr bool can_append_v = false;

    template<
        typename TServiceProvider1,
        typename TServiceProvider2
    > static constexpr bool can_append_v<
        TServiceProvider1,
        TServiceProvider2,
        std::void_t<decltype(std::declval<TServiceProvider1>() + std::declval<TServiceProvider2>())>
        > = true;

    template<
        typename TServiceProvider1,
        typename TServiceProvider2
    > static constexpr bool can_append(
        const TServiceProvider1&,
        const TServiceProvider2&
    )
    {
        return can_append_v<TServiceProvider1, TServiceProvider2>;
    }

    // Some static tests for overriding services

    // Verify that GetCompatibleIndicesForOverrides can work on empty lists.
    static_assert(
        std::is_same_v<
        std::integer_sequence<size_t>,
        decltype(detail::GetCompatibleIndicesForOverrides(
            detail::service_list_type<>(),
            detail::service_list_type<>()
        ))
        >);

    // Verify that GetCompatibleIndicesForOverrides can with an empty override list.
    static_assert(
        std::is_same_v<
        std::integer_sequence<size_t, 0>,
        decltype(detail::GetCompatibleIndicesForOverrides(
            detail::service_list_type<BaseService<0>*>(),
            detail::service_list_type<>()
        ))
        >);

    // Verify that GetCompatibleIndicesForOverrides can with overlap between the lists.
    static_assert(
        std::is_same_v<
        std::integer_sequence<size_t, 0, 2>,
        decltype(detail::GetCompatibleIndicesForOverrides(
            detail::service_list_type<
                BaseService<0>*,
                BaseService<1>*,
                BaseService<2>*
            >(),
            detail::service_list_type<
                BaseService<1>*,
                BaseService<4>*
            >()
        ))
        >);
};

TEST_F(service_provider_tests, Can_make_empty_instance)
{
    service_provider<> provider;
}

TEST_F(service_provider_tests, Can_make_instance_with_one_service)
{
    BaseService<1> baseService;
    auto provider = service_provider{ &baseService };

    assert_has_service(&baseService, provider);
}

TEST_F(service_provider_tests, Can_make_instance_with_two_services)
{
    BaseService<1> baseService1;
    BaseService<2> baseService2;
    auto provider = service_provider
    { 
        &baseService1,
        &baseService2,
    };

    assert_has_service(&baseService1, provider);
    assert_has_service(&baseService2, provider);
}

TEST_F(service_provider_tests, empty_instance_doesnt_have_unrelated_service)
{
    service_provider<> provider;
    assert_doesnt_have_service<BaseService<1>*>(provider);
}

TEST_F(service_provider_tests, instance_doesnt_have_unrelated_service)
{
    BaseService<1> baseService1;
    auto provider = service_provider
    {
        &baseService1,
    };

    assert_has_service(
        &baseService1,
        provider);
    
    assert_doesnt_have_service<BaseService<2>*>(provider);
}

TEST_F(service_provider_tests, Can_restrict_services)
{
    BaseService<1> baseService1;
    BaseService<2> baseService2;
    auto provider = service_provider
    {
        &baseService1,
        &baseService2,
    };

    service_provider<BaseService<1>*> restricted = provider;

    ASSERT_EQ(
        &baseService1,
        get<BaseService<1>*>(restricted)
    );

    assert_doesnt_have_service<BaseService<2>*>(restricted);
}

TEST_F(service_provider_tests, Can_append_services)
{
    BaseService<1> baseService1;
    BaseService<2> baseService2;

    auto provider = service_provider
    {
        &baseService1,
        &baseService2,
    };

    BaseService<3> baseService3;
    BaseService<4> baseService4;

    auto extendedProvider = provider
        +
        service_provider
    {
        &baseService3,
        &baseService4,
    };

    assert_has_service(&baseService1, extendedProvider);
    assert_has_service(&baseService2, extendedProvider);
    assert_has_service(&baseService3, extendedProvider);
    assert_has_service(&baseService4, extendedProvider);
}

TEST_F(service_provider_tests, can_append_detects_appendable_providers)
{
    BaseService<1> baseService1;
    BaseService<2> baseService2;

    auto provider12 = service_provider
    {
        &baseService1,
        &baseService2,
    };

    BaseService<3> baseService3;
    BaseService<4> baseService4;

    auto provider34 = service_provider
    {
        &baseService3,
        &baseService4,
    };

    static_assert(
        can_append(provider12, provider34));
}

TEST_F(service_provider_tests, can_append_detects_unappendable_providers)
{
    BaseService<1> baseService1;
    BaseService<2> baseService2;

    auto provider12 = service_provider
    {
        &baseService1,
        &baseService2,
    };

    BaseService<3> baseService3;

    auto provider13 = service_provider
    {
        &baseService1,
        &baseService3,
    };

    static_assert(
        !can_append(provider12, provider13));
}


TEST_F(service_provider_tests, Can_override_services)
{
    BaseService<1> baseService1;
    BaseService<2> baseService2_1;

    auto provider = service_provider
    {
        &baseService1,
        & baseService2_1,
    };

    BaseService<2> baseService2_2;
    BaseService<3> baseService3;
    BaseService<4> baseService4;

    auto extendedProvider = provider
        /
        service_provider
    {
        &baseService2_2,
        &baseService3,
        &baseService4,
    };

    assert_has_service(&baseService1, extendedProvider);
    assert_has_service(&baseService2_2, extendedProvider);
    assert_has_service(&baseService3, extendedProvider);
    assert_has_service(&baseService4, extendedProvider);
}

}