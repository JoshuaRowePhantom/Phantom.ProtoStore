#include "StandardIncludes.h"
#include "Phantom.System/async_ring_buffer.h"
#include <cppcoro/async_scope.hpp>

namespace Phantom
{
TEST(async_ring_buffer_tests, CanCreateAndDestroy)
{
    async_ring_buffer<int> buffer(1);
}

TEST(async_ring_buffer_tests, CanCompleteEmptyBuffer)
{
    run_async([]()->cppcoro::task<>
    {
        async_ring_buffer<int> buffer(1);

        co_await buffer.complete();

        auto enumeration = buffer.enumerate();
        auto iterator = co_await enumeration.begin();
        EXPECT_EQ(iterator, enumeration.end());
    });
}

TEST(async_ring_buffer_tests, CanPublishUntilBufferIsFull_Then_Can_Enumerate)
{
    run_async([]()->cppcoro::task<>
    {
        async_ring_buffer<int> buffer(4);

        cppcoro::async_scope asyncScope;

        auto addValueLambda = [&](
            int value)
            -> cppcoro::shared_task<>
        {
            co_await buffer.push(
                value);
        };

        auto task_0 = addValueLambda(0);
        auto task_1 = addValueLambda(1);
        auto task_2 = addValueLambda(2);
        auto task_3 = addValueLambda(3);
        auto task_4 = addValueLambda(4);

        asyncScope.spawn(task_0);
        asyncScope.spawn(task_1);
        asyncScope.spawn(task_2);
        asyncScope.spawn(task_3);
        asyncScope.spawn(task_4);

        EXPECT_EQ(true, task_0.is_ready());
        EXPECT_EQ(true, task_1.is_ready());
        EXPECT_EQ(true, task_2.is_ready());
        EXPECT_EQ(true, task_3.is_ready());
        EXPECT_EQ(false, task_4.is_ready());

        auto enumeration = buffer.enumerate();
        auto iterator = co_await enumeration.begin();
        EXPECT_EQ(0, *iterator);

        EXPECT_EQ(true, task_0.is_ready());
        EXPECT_EQ(true, task_1.is_ready());
        EXPECT_EQ(true, task_2.is_ready());
        EXPECT_EQ(true, task_3.is_ready());
        EXPECT_EQ(false, task_4.is_ready());

        co_await ++iterator;
        EXPECT_EQ(1, *iterator);

        EXPECT_EQ(true, task_0.is_ready());
        EXPECT_EQ(true, task_1.is_ready());
        EXPECT_EQ(true, task_2.is_ready());
        EXPECT_EQ(true, task_3.is_ready());
        EXPECT_EQ(true, task_4.is_ready());

        co_await ++iterator;
        EXPECT_EQ(2, *iterator);

        co_await ++iterator;
        EXPECT_EQ(3, *iterator);

        co_await ++iterator;
        EXPECT_EQ(4, *iterator);

        co_await buffer.complete();
        co_await ++iterator;
        EXPECT_EQ(enumeration.end(), iterator);

        co_await asyncScope.join();
    });
}


}
