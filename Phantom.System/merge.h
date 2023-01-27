#include <exception>
#include <functional>
#include <queue>
#include <utility>
#include <cppcoro/async_generator.hpp>
#include <cppcoro/async_mutex.hpp>
#include <cppcoro/async_scope.hpp>
#include <cppcoro/async_auto_reset_event.hpp>
#include <cppcoro/task.hpp>
#include "async_utility.h"

namespace Phantom
{

template<
    typename TItem,
    typename TBeginIterator,
    typename TEndIterator,
    typename TComparer = std::less<TItem>
>
cppcoro::async_generator<TItem> merge_sorted_generators(
    TBeginIterator beginIterator,
    TEndIterator endIterator,
    TComparer comparer = TComparer()
)
{
    cppcoro::async_mutex mutex;
    cppcoro::async_scope scope;
    cppcoro::async_auto_reset_event itemProducedEvent;
    size_t notReadyGeneratorCount = 0;
    std::exception_ptr exception;

    struct entry
    {
        TItem* item;
        cppcoro::async_auto_reset_event* itemConsumedEvent;
    };

    auto entryComparer = [&](
        const entry& left,
        const entry& right
        )
    {
        return comparer(
            *left.item,
            *right.item
        );
    };

    std::priority_queue<entry, std::vector<entry>, decltype(entryComparer)> entries(
        entryComparer);

    auto processGeneratorLambda = [&](
        auto& generator
        ) -> cppcoro::task<>
    {
        std::exception_ptr localException;

        try
        {
            cppcoro::async_auto_reset_event itemConsumedEvent;

            for (auto iterator = co_await generator.begin();
                iterator != generator.end();
                co_await ++iterator)
            {
                {
                    auto lock = co_await mutex.scoped_lock_async();
                    
                    if (exception)
                    {
                        break;
                    }

                    entries.push(
                        {
                            std::addressof(*iterator),
                            &itemConsumedEvent
                        });
                    notReadyGeneratorCount--;

                    itemProducedEvent.set();
                }
            }
        }
        catch (...)
        {
            localException = std::current_exception();
        }

        auto lock = co_await mutex.scoped_lock_async();
        if (localException)
        {
            exception = localException;
        }
        notReadyGeneratorCount--;
        itemProducedEvent.set();
    };

    {
        auto lock = co_await mutex.scoped_lock_async();
        for (auto generatorIterator = beginIterator; generatorIterator != endIterator; generatorIterator++)
        {
            scope.spawn(processGeneratorLambda(*generatorIterator));
            ++notReadyGeneratorCount;
        }
    }

    while(true)
    {
        co_await itemProducedEvent;

        {
            auto lock = co_await mutex.scoped_lock_async();
            if (notReadyGeneratorCount > 0)
            {
                continue;
            }

            if (entries.empty())
            {
                co_await scope.join();
                if (exception)
                {
                    std::rethrow_exception(exception);
                }
                co_return;
            }

            co_yield *entries.top().item;
            entries.pop();
        }
    }
}

}