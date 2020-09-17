#include "StandardIncludes.h"
#include "Phantom.System/async_utility.h"

namespace Phantom
{

TEST(as_awaitable_tests, as_awaitable_can_return_ordinary_integer_value_without_waiting)
{
    auto awaitable = as_awaitable(5);
    ASSERT_TRUE(awaitable.await_ready());
    ASSERT_EQ(5, awaitable.await_resume());
}

struct as_awaitable_test_move_only_class : public std::string
{
    as_awaitable_test_move_only_class(
        std::string value
    ) : std::string(value)
    {}

    as_awaitable_test_move_only_class(
        const as_awaitable_test_move_only_class&
    ) = delete;

    as_awaitable_test_move_only_class(
        as_awaitable_test_move_only_class&& other
    ) noexcept : std::string(other)
    {}
};

TEST(as_awaitable_tests, as_awaitable_can_return_move_only_class)
{
    auto awaitable = as_awaitable(as_awaitable_test_move_only_class("foo"));
    ASSERT_TRUE(awaitable.await_ready());
    auto value = move(awaitable).await_resume();
    ASSERT_EQ(std::string("foo"), std::string(value));
}

TEST(as_awaitable_tests, as_awaitable_can_return_ordinary_class)
{
    std::string stringValue("foo");
    auto awaitable = as_awaitable(stringValue);
    ASSERT_TRUE(awaitable.await_ready());
    auto& value = move(awaitable).await_resume();
    ASSERT_EQ(std::string("foo"), value);
}

}