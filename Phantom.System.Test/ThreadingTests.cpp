#include "gtest/gtest.h"
#include "ThreadingTests.h"
#include <string>
#include <experimental/coroutine>

TEST(ThreadingTests, First)
{
    ASSERT_TRUE(false) << "Successfully failed!";
}

struct simple_task
{
    struct promise_type
    {
        std::experimental::suspend_never initial_suspend()
        {
            return {};
        }

        std::experimental::suspend_never final_suspend() noexcept
        {
            return {};
        }

        void unhandled_exception()
        {}

        simple_task get_return_object()
        {
            return {};
        }

        void return_void()
        {}
    };

    bool await_ready() const
    {
        return false;
    }

    void await_resume()
    {
    }

    void await_suspend(
        std::experimental::coroutine_handle<> coroutineHandle)
    {
        coroutineHandle.resume();
    }
};

simple_task AsyncRoutine2(
    std::string something)
{
    std::cout << something << "\r\n";
    co_return;
}

simple_task AsyncRoutine1()
{
    co_await AsyncRoutine2(
        "AsyncRoutine2 1");
    co_await AsyncRoutine2(
        "AsyncRoutine2 2");
}

TEST(ThreadingTests, coroutines_1)
{
    auto task = AsyncRoutine1();
}