#include "StandardIncludes.h"
#include "Phantom.System/merge.h"
#include <string>
#include <vector>
#include <cppcoro/task.hpp>
#include <experimental/generator>
namespace Phantom
{

template<
    typename TItem,
    typename TContainer
> cppcoro::task<std::vector<TItem>> ToVector(
    TContainer container)
{
    std::vector<TItem> result;
    auto end = container.end();

    for (auto iterator = co_await container.begin(); iterator != end; co_await ++iterator)
    {
        result.push_back(*iterator);
    }

    co_return result;
}

TEST(merge_tests, merge_sorted_generators_can_merge_empty_sequence)
{
    run_async([]()->cppcoro::task<>
    {
        std::vector<cppcoro::async_generator<int>> generators;

        std::vector<int> actualResult = co_await ToVector<int>(merge_sorted_generators<int>(
            generators.begin(),
            generators.end()
        ));

        auto expectedResult = std::vector<int>();
        EXPECT_EQ(expectedResult, actualResult);
    });
}

TEST(merge_tests, merge_sorted_generators_can_merge_multiple_sequences)
{
    run_async([]()->cppcoro::task<>
    {
        std::vector<cppcoro::async_generator<int>> generators;
        generators.emplace_back(
            []() -> cppcoro::async_generator<int>
        {
            co_yield 2;
            co_yield 5;
            co_yield 6;
            co_yield 7;
        }());
        generators.emplace_back(
            cppcoro::async_generator<int>());
        generators.emplace_back(
            []() -> cppcoro::async_generator<int>
        {
            co_yield 1;
            co_yield 5;
            co_yield 9;
        }());

        std::vector<int> actualResult = co_await ToVector<int>(merge_sorted_generators<int>(
            generators.begin(),
            generators.end()
            ));

        auto expectedResult = std::vector<int> 
        {
            1,
            2,
            5,
            5,
            6,
            7,
            9,
        };

        EXPECT_EQ(expectedResult, actualResult);
    });
}

}