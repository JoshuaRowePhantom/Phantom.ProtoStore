#include "StandardIncludes.h"
#include <cppcoro/async_manual_reset_event.hpp>
#include <cppcoro/task.hpp>
#include <cppcoro/when_all.hpp>
#include "Phantom.System/async_value_source.h"

using cppcoro::task;

namespace Phantom
{

class value_publisher_tests : public ::testing::Test
{
protected:
    static int sm_nConstructorCalledCount;
    static int sm_nDestructorCalledCount;
public:
    class ConstructorAndDestructorTracker
    {
    public:
        ConstructorAndDestructorTracker()
        {
            sm_nConstructorCalledCount++;
        }

        ~ConstructorAndDestructorTracker()
        {
            sm_nDestructorCalledCount++;
        }
    };

    value_publisher_tests()
    {
        sm_nConstructorCalledCount = 0;
        sm_nDestructorCalledCount = 0;
    }
};

int value_publisher_tests::sm_nConstructorCalledCount = 0;
int value_publisher_tests::sm_nDestructorCalledCount = 0;

TEST_F(value_publisher_tests, does_not_destroy_unconstructed_value)
{
    ASSERT_EQ(0, sm_nConstructorCalledCount);
    ASSERT_EQ(0, sm_nDestructorCalledCount);

    {
        async_value_source<ConstructorAndDestructorTracker> value;
    }

    ASSERT_EQ(0, sm_nConstructorCalledCount);
    ASSERT_EQ(0, sm_nDestructorCalledCount);
}

TEST_F(value_publisher_tests, destroys_constructed_value)
{
    {
        ASSERT_EQ(0, sm_nConstructorCalledCount);
        ASSERT_EQ(0, sm_nDestructorCalledCount);

        async_value_source<ConstructorAndDestructorTracker> value;
        value.emplace();

        ASSERT_EQ(1, sm_nConstructorCalledCount);
        ASSERT_EQ(0, sm_nDestructorCalledCount);
    }

    ASSERT_EQ(1, sm_nConstructorCalledCount);
    ASSERT_EQ(1, sm_nDestructorCalledCount);
}

TEST_F(value_publisher_tests, await_ready_true_after_emplace)
{
    async_value_source<std::string> publisher;
    auto awaiter = publisher.operator co_await();
    ASSERT_EQ(false, awaiter.await_ready());
    publisher.emplace("foo");
    ASSERT_EQ(true, awaiter.await_ready());
}

TEST_F(value_publisher_tests, can_get_value_after_set_initially)
{
    run_async([]()->task<>
    {
        async_value_source<std::string> publisher;
        publisher.emplace("foo");
        ASSERT_EQ(co_await publisher, "foo");
    });
}

TEST_F(value_publisher_tests, can_get_value_before_set_initially)
{
    run_async([]()->task<>
    {
        async_value_source<std::string> publisher;
        auto task1 = [&]()->task<>
        {
            ASSERT_EQ(false, publisher.is_set());
            ASSERT_EQ(co_await publisher, "foo");
        } ();
        auto task2 = [&]()->task<>
        {
            publisher.emplace("foo");
            co_return;
        } ();
        co_await cppcoro::when_all(
            std::move(task1),
            std::move(task2));
    });
}

}
