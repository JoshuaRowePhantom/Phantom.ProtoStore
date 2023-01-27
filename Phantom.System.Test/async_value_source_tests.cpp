#include "StandardIncludes.h"
#include <cppcoro/async_manual_reset_event.hpp>
#include <cppcoro/task.hpp>
//#include <cppcoro/when_all.hpp>
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
    EXPECT_EQ(0, sm_nConstructorCalledCount);
    EXPECT_EQ(0, sm_nDestructorCalledCount);

    {
        async_value_source<ConstructorAndDestructorTracker> value;
    }

    EXPECT_EQ(0, sm_nConstructorCalledCount);
    EXPECT_EQ(0, sm_nDestructorCalledCount);
}

TEST_F(value_publisher_tests, destroys_constructed_value)
{
    {
        EXPECT_EQ(0, sm_nConstructorCalledCount);
        EXPECT_EQ(0, sm_nDestructorCalledCount);

        async_value_source<ConstructorAndDestructorTracker> value;
        value.emplace();

        EXPECT_EQ(1, sm_nConstructorCalledCount);
        EXPECT_EQ(0, sm_nDestructorCalledCount);
    }

    EXPECT_EQ(1, sm_nConstructorCalledCount);
    EXPECT_EQ(1, sm_nDestructorCalledCount);
}

TEST_F(value_publisher_tests, can_get_value_after_set_initially)
{
    run_async([]()->task<>
    {
        async_value_source<std::string> publisher;
        publisher.emplace("foo");
        EXPECT_EQ(co_await publisher.wait(), "foo");
    });
}

TEST_F(value_publisher_tests, can_get_exception_after_exception_initially)
{
    run_async([]()->task<>
    {
        async_value_source<std::string> publisher;
        try
        {
            throw std::range_error("foo");
        }
        catch (...)
        {
            publisher.unhandled_exception();
        }
        EXPECT_THROW(
            co_await publisher.wait(), 
            std::range_error);
    });
}

TEST_F(value_publisher_tests, can_get_value_before_set_initially)
{
    run_async([]()->task<>
    {
        async_value_source<std::string> publisher;
        auto task1 = [&]()->task<>
        {
            EXPECT_EQ(false, publisher.is_set());
            EXPECT_EQ(co_await publisher.wait(), "foo");
        } ();
        auto task2 = [&]()->task<>
        {
            publisher.emplace("foo");
            co_return;
        } ();
        co_await task1;
        co_await task2;
    });
}

TEST_F(value_publisher_tests, can_get_exception_before_set_initially)
{
    run_async([]()->task<>
    {
        async_value_source<std::string> publisher;
        auto task1 = [&]()->task<>
        {
            EXPECT_EQ(false, publisher.is_set());
            EXPECT_THROW(
                co_await publisher.wait(),
                std::range_error);
        } ();
        auto task2 = [&]()->task<>
        {
            try
            {
                throw std::range_error("foo");
            }
            catch (...)
            {
                publisher.unhandled_exception();
            }
            co_return;
        } ();
        co_await task1;
        co_await task2;
    });
}

}
