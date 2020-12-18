#include "StandardIncludes.h"
#include "Phantom.System/scope.h"

namespace Phantom
{
TEST(scope_tests, Calls_function_at_scope_exit)
{
    int calledCounter = 0;

    {
        scope_exit scope([&]()
        {
            ++calledCounter;
        });
    }

    ASSERT_EQ(1, calledCounter);
}

TEST(scope_tests, Doesnt_call_function_if_released)
{
    int calledCounter = 0;

    {
        scope_exit scope([&]()
        {
            ++calledCounter;
        });

        scope.release();
    }

    ASSERT_EQ(0, calledCounter);
}


TEST(scope_tests, Doesnt_call_after_moved)
{
    int calledCounter = 0;

    auto acceptingLambda = [&](auto scope)
    {
    };

    auto lambda = [&]()
    {
        return scope_exit(
            [&]()
        {
            ++calledCounter;
        });
    };

    {
        auto scope = lambda();
        ASSERT_EQ(0, calledCounter);
        acceptingLambda(
            std::move(scope));
        ASSERT_EQ(1, calledCounter);
    }

    ASSERT_EQ(1, calledCounter);
}

TEST(scope_tests, Doesnt_call_after_assigned)
{
    int calledCounter = 0;

    {
        scope_exit scope1 = [&]()
        {
            ++calledCounter;
        };
        auto scope2 = std::move(scope1);
    }

    ASSERT_EQ(1, calledCounter);
}


}