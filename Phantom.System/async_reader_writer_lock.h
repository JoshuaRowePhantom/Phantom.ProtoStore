#pragma once

#include "Phantom.Coroutines/async_reader_writer_lock.h"
#include "Phantom.Coroutines/suspend_result.h"
#include "cppcoro/task.hpp"

namespace Phantom
{
using async_reader_writer_lock = ::Phantom::Coroutines::async_reader_writer_lock<>;

template<
    typename TScheduler,
    typename TReadLambda,
    typename TWriteLambda
>
cppcoro::task<> execute_conditional_read_unlikely_write_operation(
    async_reader_writer_lock& reader_writer_lock,
    TScheduler& scheduler,
    TReadLambda readLambda,
    TWriteLambda writeLambda
)
{
    {
        Phantom::Coroutines::suspend_result suspendResult;
        auto readlock = co_await (suspendResult << reader_writer_lock.reader().scoped_lock_async());
        if (suspendResult.did_suspend())
        {
            co_await scheduler.schedule();
        }

        if (co_await readLambda(false))
        {
            co_return;
        }
    }

    {
        auto writeLock = co_await reader_writer_lock.reader().scoped_lock_async();

        if (co_await readLambda(false))
        {
            co_return;
        }

        co_await writeLambda();
    }
}

}