#include "StandardIncludes.h"
#include "Phantom.System/encompassing_pending_task.h"
#include <cppcoro/async_auto_reset_event.hpp>
#include <cppcoro/async_scope.hpp>

namespace Phantom
{

TEST(encompassing_pending_task_tests, join_on_not_spawned_returns_right_away)
{
    run_async([]() -> cppcoro::task<>
    {
        encompassing_pending_task task;

        co_await task.join();
    });
}

TEST(encompassing_pending_task_tests, spawn_immediately_starts_new_tasks_that_dont_complete_until_precedents_have_completed)
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

        auto makeEncompassedTask = [&]() -> cppcoro::task<>
        {
            ++taskGeneratorCalledCount;
            return taskRunner();
        };

        encompassing_pending_task task;

        EXPECT_EQ(0, taskGeneratorCalledCount);

        auto task1 = cppcoro::make_shared_task(
            task.spawn(
                makeEncompassedTask()));
        asyncScope.spawn(task1);

        EXPECT_EQ(1, taskGeneratorCalledCount);
        EXPECT_EQ(1, taskExecutionCalledCount);

        auto task2 = cppcoro::make_shared_task(
            task.spawn(
                makeEncompassedTask()));

        asyncScope.spawn(task2);

        auto task3 = cppcoro::make_shared_task(
            task.spawn(
                makeEncompassedTask()));

        asyncScope.spawn(task3);

        EXPECT_EQ(3, taskGeneratorCalledCount);
        EXPECT_EQ(3, taskExecutionCalledCount);
        
        EXPECT_EQ(false, task1.is_ready());
        EXPECT_EQ(false, task2.is_ready());
        EXPECT_EQ(false, task3.is_ready());

        proceedEvent.set();

        EXPECT_EQ(true, task1.is_ready());
        EXPECT_EQ(false, task2.is_ready());
        EXPECT_EQ(false, task3.is_ready());

        co_await task1;

        EXPECT_EQ(3, taskGeneratorCalledCount);
        EXPECT_EQ(3, taskExecutionCalledCount);

        proceedEvent.set();

        EXPECT_EQ(true, task1.is_ready());
        EXPECT_EQ(true, task2.is_ready());
        EXPECT_EQ(false, task3.is_ready());

        co_await task2;

        proceedEvent.set();

        EXPECT_EQ(true, task1.is_ready());
        EXPECT_EQ(true, task2.is_ready());
        EXPECT_EQ(true, task3.is_ready());

        co_await task.join();

        co_await asyncScope.join();
    });
}
}