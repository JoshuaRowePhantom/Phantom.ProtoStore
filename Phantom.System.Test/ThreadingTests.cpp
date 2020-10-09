#include "gtest/gtest.h"
#include "ThreadingTests.h"
#include <string>

TEST(ThreadingTests, First)
{
    EXPECT_TRUE(true);
}

#include <cppcoro/when_all.hpp>
#include <cppcoro/task.hpp>

TEST(test_whenall, foo)
{

}