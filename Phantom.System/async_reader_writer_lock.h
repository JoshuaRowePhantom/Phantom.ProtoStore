#pragma once

#include "Phantom.Coroutines/async_reader_writer_lock.h"
#include "Phantom.Coroutines/scheduler.h"
#include "Phantom.Coroutines/suspend_result.h"
#include "cppcoro/task.hpp"

namespace Phantom
{
using async_reader_writer_lock = ::Phantom::Coroutines::async_reader_writer_lock<>;

// Execute an action under a readlock that is unlikely to require
// a write lock, but might require a write lock.
// If the async TLambda returns true, the action is complete.
// If the async TLambda returns false, the action is not complete,
// the lambda is executed in a loop aquiring first the read lock,
// then the write lock, then the read lock, etc., until the operation
// succeeds.
// Whenever the lock acquisition suspends, the scheduler is used to
// reschedule the continuation. This preserves parallelism.
template<
    std::invocable<bool> TLambda,
    template <typename> typename Task = cppcoro::task
>
Task<void> execute_conditional_read_unlikely_write_operation(
    auto& reader_writer_lock,
    Phantom::Coroutines::is_scheduler auto& scheduler,
    TLambda lambda
)
{
    while (true)
    {
        {
            Phantom::Coroutines::suspend_result suspendResult;
            auto readlock = co_await(suspendResult << reader_writer_lock.reader().scoped_lock_async());
            if (suspendResult.did_suspend())
            {
                co_await scheduler.schedule();
            }

            if (co_await lambda(false))
            {
                co_return;
            }
        }

        {
            Phantom::Coroutines::suspend_result suspendResult;
            auto writeLock = co_await(suspendResult << reader_writer_lock.writer().scoped_lock_async());
            if (suspendResult.did_suspend())
            {
                co_await scheduler.schedule();
            }

            if (co_await lambda(true))
            {
                co_return;
            }
        }
    }
}

template<
    std::invocable<bool> TReadLambda,
    std::invocable TWriteLambda,
    template <typename> typename Task = cppcoro::task
>
Task<void> execute_conditional_read_unlikely_write_operation(
    auto& reader_writer_lock,
    Phantom::Coroutines::is_scheduler auto& scheduler,
    TReadLambda&& readLambda,
    TWriteLambda&& writeLambda
)
{
    return execute_conditional_read_unlikely_write_operation(
        reader_writer_lock,
        scheduler,
        [
            readLambda = std::forward<TReadLambda>(readLambda), 
            writeLambda = std::forward<TWriteLambda>(writeLambda)
        ](bool hasWriteLock) -> Task<bool>
    {
        if (!co_await readLambda(hasWriteLock))
        {
            if (!hasWriteLock)
            {
                co_return false;
            }
            else
            {
                co_await writeLambda();
            }
        }
    }
    );
}

}