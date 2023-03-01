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

}