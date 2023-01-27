#include "StandardIncludes.h"
#include "Phantom.Scalable/Scheduling.h"
#include <cppcoro/async_manual_reset_event.hpp>

namespace Phantom::Scalable
{

TEST(BackgroundWorkerTests, can_create_and_destroy)
{
    auto backgroundWorker = std::make_shared<BackgroundWorker>();
}

TEST(BackgroundWorkerTests, can_spawn_and_create_and_destroy_without_joining)
{
    auto backgroundWorker = std::make_shared<BackgroundWorker>();
    backgroundWorker->spawn(
        []() -> cppcoro::task<>
    {
        co_return;
    }()
    );
}

TEST(BackgroundWorkerTests, can_spawn_and_create_and_destroy_without_joining_with_task_that_last_longer_than_lifetime_of_BackgroundWorker)
{
    cppcoro::async_manual_reset_event event;
    cppcoro::shared_task<> task = [&event]()->cppcoro::shared_task<>
    {
        co_await event;
    }();
    cppcoro::shared_task<> joinTask;

    {
        auto backgroundWorker = std::make_shared<BackgroundWorker>();
        backgroundWorker->spawn(
            task);
        // Note that we never await the join task.
        joinTask = backgroundWorker->join();
    }

    ASSERT_FALSE(joinTask.is_ready());
    ASSERT_FALSE(task.is_ready());
    event.set();
    ASSERT_TRUE(joinTask.is_ready());
    ASSERT_TRUE(task.is_ready());
}

TEST(BackgroundWorkerTests, can_spawn_and_create_and_destroy_with_joining_with_task_that_last_longer_than_lifetime_of_BackgroundWorker)
{
    run_async([]()->cppcoro::task<>
    {
        cppcoro::async_manual_reset_event event;
        cppcoro::shared_task<> task = [&event]()->cppcoro::shared_task<>
        {
            co_await event;
        }();
        cppcoro::shared_task<> joinTask;
        cppcoro::async_scope joinScope;

        {
            auto backgroundWorker = std::make_shared<BackgroundWorker>();
            backgroundWorker->spawn(
                task);
            joinTask = backgroundWorker->join();
            joinScope.spawn(
                joinTask);
        }

        EXPECT_FALSE(joinTask.is_ready());
        EXPECT_FALSE(task.is_ready());
        event.set();
        EXPECT_TRUE(joinTask.is_ready());
        EXPECT_TRUE(task.is_ready());
        co_await joinScope.join();
    });
}

TEST(BackgroundWorkerTests, can_spawn_and_create_and_destroy_with_joining_before_starting_task_that_last_longer_than_lifetime_of_BackgroundWorker)
{
    run_async([]()->cppcoro::task<>
    {
        cppcoro::async_manual_reset_event event;
        cppcoro::shared_task<> task = [&event]()->cppcoro::shared_task<>
        {
            co_await event;
        }();
        cppcoro::shared_task<> joinTask;
        cppcoro::async_scope joinScope;

        {
            auto backgroundWorker = std::make_shared<BackgroundWorker>();
            joinTask = backgroundWorker->join();
            joinScope.spawn(
                joinTask);

            backgroundWorker->spawn(
                task);
        }

        EXPECT_FALSE(joinTask.is_ready());
        EXPECT_FALSE(task.is_ready());
        event.set();
        EXPECT_TRUE(joinTask.is_ready());
        EXPECT_TRUE(task.is_ready());
        co_await joinScope.join();
    });
}

}