#include "StandardIncludes.h"
#include "Phantom.System/async_utility.h"
#include <vector>

namespace Phantom
{

TEST(async_lower_bound_tests, can_find_lower_bound_in_empty_collection)
{
    run_async([]() -> cppcoro::task<>
    {
        std::vector<int> v;
        auto result = co_await async_lower_bound(
            v.begin(),
            v.end(),
            0,
            std::less<int>());

        ASSERT_EQ(
            v.begin(),
            result);
    });
}

TEST(async_lower_bound_tests, can_find_lower_bound_in_non_empty_collection)
{
    run_async([]() -> cppcoro::task<>
    {
        std::vector<int> v =
        {
            1,
            3,
            5,
        };

        {
            auto result = co_await async_lower_bound(
                v.begin(),
                v.end(),
                0,
                std::less<int>());

            ASSERT_EQ(
                v.begin() + 0,
                result);
        }

        {
            auto result = co_await async_lower_bound(
                v.begin(),
                v.end(),
                1,
                std::less<int>());

            ASSERT_EQ(
                v.begin() + 0,
                result);
        }

        {
            auto result = co_await async_lower_bound(
                v.begin(),
                v.end(),
                2,
                std::less<int>());

            ASSERT_EQ(
                v.begin() + 1,
                result);
        }

        {
            auto result = co_await async_lower_bound(
                v.begin(),
                v.end(),
                3,
                std::less<int>());

            ASSERT_EQ(
                v.begin() + 1,
                result);
        }

        {
            auto result = co_await async_lower_bound(
                v.begin(),
                v.end(),
                4,
                std::less<int>());

            ASSERT_EQ(
                v.begin() + 2,
                result);
        }

        {
            auto result = co_await async_lower_bound(
                v.begin(),
                v.end(),
                5,
                std::less<int>());

            ASSERT_EQ(
                v.begin() + 2,
                result);
        }

        {
            auto result = co_await async_lower_bound(
                v.begin(),
                v.end(),
                6,
                std::less<int>());

            ASSERT_EQ(
                v.begin() + 3,
                result);
        }
    });
}

TEST(async_upper_bound_tests, can_find_upper_bound_in_empty_collection)
{
    run_async([]() -> cppcoro::task<>
    {
        std::vector<int> v;
        auto result = co_await async_upper_bound(
            v.begin(),
            v.end(),
            0,
            std::less<int>());

        ASSERT_EQ(
            v.begin(),
            result);
    });
}

TEST(async_upper_bound_tests, can_find_upper_bound_in_non_empty_collection)
{
    run_async([]() -> cppcoro::task<>
    {
        std::vector<int> v =
        {
            1,
            3,
            5,
        };

        {
            auto result = co_await async_upper_bound(
                v.begin(),
                v.end(),
                0,
                std::less<int>());

            ASSERT_EQ(
                v.begin() + 0,
                result);
        }

        {
            auto result = co_await async_upper_bound(
                v.begin(),
                v.end(),
                1,
                std::less<int>());

            ASSERT_EQ(
                v.begin() + 1,
                result);
        }

        {
            auto result = co_await async_upper_bound(
                v.begin(),
                v.end(),
                2,
                std::less<int>());

            ASSERT_EQ(
                v.begin() + 1,
                result);
        }

        {
            auto result = co_await async_upper_bound(
                v.begin(),
                v.end(),
                3,
                std::less<int>());

            ASSERT_EQ(
                v.begin() + 2,
                result);
        }

        {
            auto result = co_await async_upper_bound(
                v.begin(),
                v.end(),
                4,
                std::less<int>());

            ASSERT_EQ(
                v.begin() + 2,
                result);
        }

        {
            auto result = co_await async_upper_bound(
                v.begin(),
                v.end(),
                5,
                std::less<int>());

            ASSERT_EQ(
                v.begin() + 3,
                result);
        }

        {
            auto result = co_await async_upper_bound(
                v.begin(),
                v.end(),
                6,
                std::less<int>());

            ASSERT_EQ(
                v.begin() + 3,
                result);
        }
    });
}

}