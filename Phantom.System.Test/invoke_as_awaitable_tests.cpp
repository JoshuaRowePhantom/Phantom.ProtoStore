#include "StandardIncludes.h"
#include "Phantom.System/async_utility.h"

namespace Phantom
{

TEST(invoke_as_awaitable_tests, can_invoke_nonasync_lambda)
{
    run_async(
        []() -> cppcoro::task<>
    {
        auto lambda = []() { return 5; };

        auto result = co_await invoke_as_awaitable(
            lambda);

        ASSERT_EQ(5, result);
    });
}

TEST(invoke_as_awaitable_tests, can_invoke_async_lambda)
{
    run_async(
        []() -> cppcoro::task<>
    {
        auto lambda = []() -> cppcoro::task<int>{ co_return 5; };

        auto result = co_await invoke_as_awaitable(
            lambda);

        ASSERT_EQ(5, result);
    });
}

TEST(invoke_as_awaitable_tests, can_forward_arguments)
{
    run_async(
        []() -> cppcoro::task<>
    {
        auto lambda = [](
            cppcoro::task<int> t
            ) -> cppcoro::task<int> { co_return co_await t; };

        auto result = co_await invoke_as_awaitable(
            lambda,
            []() -> cppcoro::task<int> { co_return 5; }());

        ASSERT_EQ(5, result);
    });
}
}