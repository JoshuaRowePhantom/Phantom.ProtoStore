#pragma once

#include <functional>
#include <cppcoro/async_mutex.hpp>
#include <cppcoro/task.hpp>
#include <cppcoro/shared_task.hpp>
#include "async_utility.h"

namespace Phantom
{
// This class ensures that there is a maximum of one running
// invocation of a task, and coalesces new requests to run
// the task into a single request.
// The intent is that each task invocation completely
// consumes some state, with spawn() indicating that there
// is potentially more state to consume in the next invocation,
// so that a new invocation should be started after the
// current one has completed.  Multiple calls to spawn()
// before the current one has completed will result in
// only a single invocation, as that invocation will
// consume all the new state.
class single_pending_task
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

    single_pending_task(
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

        co_await lastTask.when_ready();
        co_await nextTask;
    }

    cppcoro::task<> join()
    {
        auto lock = co_await m_mutex.scoped_lock_async();
        co_await m_lastTask;
    }
};

}