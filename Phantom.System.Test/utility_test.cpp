#include <gtest/gtest.h>
#include "Phantom.System/lifetime_tracker.h"
#include "Phantom.System/utility.h"

namespace Phantom
{

TEST(copy_unique_test, makes_copy_of_value_type)
{
    lifetime_statistics statistics;
    std::unique_ptr<lifetime_tracker> copy = copy_unique(statistics.tracker());
    EXPECT_EQ(*copy, statistics);
}

TEST(copy_unique_test, makes_copy_of_reference_type)
{
    lifetime_statistics statistics;
    auto tracker = statistics.tracker();
    std::unique_ptr<lifetime_tracker> copy = copy_unique(tracker);
    EXPECT_EQ(*copy, statistics);
}

TEST(copy_shared_test, makes_copy_of_value_type)
{
    lifetime_statistics statistics;
    std::shared_ptr<lifetime_tracker> copy = copy_shared(statistics.tracker());
    EXPECT_EQ(*copy, statistics);
}

TEST(copy_shared_test, makes_copy_of_reference_type)
{
    lifetime_statistics statistics;
    auto tracker = statistics.tracker();
    std::shared_ptr<lifetime_tracker> copy = copy_shared(tracker);
    EXPECT_EQ(*copy, statistics);
}

TEST(value_factory_test, returns_convertible_object)
{
    auto valueFactory = value_factory([] { return "hello world"; });
    std::string value = valueFactory;
    EXPECT_EQ(value, "hello world");
}

TEST(make_shared_with_strong_reference_test, moves_lifetime_object_once_and_makes_result_without_copying_or_moving)
{
    lifetime_statistics lifetimeStatistics;
    lifetime_statistics argumentStatistics;
    
    struct value
    {
        lifetime_tracker argumentTracker;

        value(
            lifetime_tracker&& arg
        ) : argumentTracker(std::move(arg))
        {}

        // non-copyable, non-movable
        value& operator=(value&&) = delete;
    };

    auto result = make_shared_with_strong_reference<value>(
        lifetimeStatistics.tracker(),
        argumentStatistics.tracker()
    );

    EXPECT_EQ(lifetimeStatistics.instance_count, 1);
    EXPECT_EQ(lifetimeStatistics.move_construction_count, 1);
    EXPECT_EQ(lifetimeStatistics.copy_construction_count, 0);

    EXPECT_EQ(argumentStatistics.instance_count, 1);
    EXPECT_EQ(argumentStatistics.move_construction_count, 1);
    EXPECT_EQ(argumentStatistics.copy_construction_count, 0);

    result.reset();
    
    EXPECT_EQ(lifetimeStatistics.instance_count, 0);
    EXPECT_EQ(argumentStatistics.instance_count, 0);

}

}