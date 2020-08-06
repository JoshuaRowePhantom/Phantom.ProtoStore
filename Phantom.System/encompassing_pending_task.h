#pragma once

#include <functional>
#include <cppcoro/async_mutex.hpp>
#include <cppcoro/task.hpp>
#include <cppcoro/shared_task.hpp>
#include "utility.h"

namespace Phantom
{
// This class ensures that each invocation of spawn()
// immediately starts a new task, but ensure that the resulting
// task completes only after all previous invocations of
// spawn() have completed.
// The intent is that each call to spawn starts some process
// that should only be reported as long as it and all previous
// calls have completed, but can otherwise proceed independently.
class encompassing_pending_task
{
public:
    typedef std::function<cppcoro::task<>()> task_generator_type;

private:
    task_generator_type m_taskGenerator;

    cppcoro::async_mutex m_mutex;
    cppcoro::shared_task<> m_lastTask;
    cppcoro::shared_task<> m_nextTask;

    cppcoro::shared_task<> delayed_run_next_task()
    {
        {
            auto lock = co_await m_mutex.scoped_lock_async();
            m_lastTask = m_nextTask;
            m_nextTask = delayed_run_next_task();
        }

        co_await m_taskGenerator();
    }

public:

    encompassing_pending_task(
        task_generator_type taskGenerator
    ) :
        m_taskGenerator(taskGenerator),
        m_lastTask(make_completed_shared_task()),
        m_nextTask(delayed_run_next_task())
    {
    }

    cppcoro::task<> spawn()
    {
        cppcoro::shared_task<> lastTask;
        cppcoro::shared_task<> nextTask;

        {
            auto lock = co_await m_mutex.scoped_lock_async();
            lastTask = m_lastTask;
            nextTask = m_nextTask;
        }

        co_await nextTask;
        co_await lastTask.when_ready();
    }

    cppcoro::task<> join()
    {
        auto lock = co_await m_mutex.scoped_lock_async();
        co_await m_lastTask;
    }
};

}