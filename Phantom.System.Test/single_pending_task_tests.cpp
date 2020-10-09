#include "StandardIncludes.h"
#include "Phantom.System/single_pending_task.h"
#include <cppcoro/async_auto_reset_event.hpp>
#include <cppcoro/async_scope.hpp>

namespace Phantom
{

TEST(single_pending_task_tests, join_on_not_spawned_returns_right_away)
{
    run_async([]() -> cppcoro::task<>
    {
        int taskGeneratorCalledCount = 0;
        single_pending_task task([&]() -> cppcoro::task<>
        {
            ++taskGeneratorCalledCount;
            co_return;
        });

        co_await task.join();
        EXPECT_EQ(0, taskGeneratorCalledCount);
    });
}

TEST(single_pending_task_tests, spawn_queues_multiple_task_runs)
{
    run_async([]() -> cppcoro::task<>
    {
        int taskGeneratorCalledCount = 0;
        int taskExecutionCalledCount = 0;

        cppcoro::async_auto_reset_event proceedEvent;
        cppcoro::async_scope asyncScope;

        auto taskRunner = [&]() -> cppcoro::task<>
        {
            ++taskExecutionCalledCount;
            co_await proceedEvent;
        };

        single_pending_task task([&]() -> cppcoro::task<>
        {
            ++taskGeneratorCalledCount;
            return taskRunner();
        });

        EXPECT_EQ(0, taskGeneratorCalledCount);

        auto task1 = cppcoro::make_shared_task(
            task.spawn());
        asyncScope.spawn(task1);

        EXPECT_EQ(1, taskGeneratorCalledCount);
        EXPECT_EQ(1, taskExecutionCalledCount);

        auto task2 = cppcoro::make_shared_task(
            task.spawn());

        asyncScope.spawn(task2);

        auto task3 = cppcoro::make_shared_task(
            task.spawn());

        asyncScope.spawn(task3);

        EXPECT_EQ(1, taskGeneratorCalledCount);
        EXPECT_EQ(1, taskExecutionCalledCount);

        proceedEvent.set();

        co_await task1;

        EXPECT_EQ(2, taskGeneratorCalledCount);
        EXPECT_EQ(2, taskExecutionCalledCount);

        proceedEvent.set();
        co_await task2;
        co_await task3;

        co_await task.join();
        EXPECT_EQ(2, taskGeneratorCalledCount);
        EXPECT_EQ(2, taskExecutionCalledCount);

        co_await asyncScope.join();
    });
}
}