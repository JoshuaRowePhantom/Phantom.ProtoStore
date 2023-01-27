#pragma once

#include "Memory.h"
#include <any>
#include <coroutine>
#include <cppcoro/async_scope.hpp>
#include <cppcoro/async_manual_reset_event.hpp>
#include <cppcoro/shared_task.hpp>
#include <cppcoro/sync_wait.hpp>
#include <cppcoro/task.hpp>

namespace Phantom::Scalable
{

class IScheduler
{
public:
    class schedule_operation
    {
        friend class IScheduler;
        IScheduler* m_scheduler;
        std::any m_value;

        schedule_operation(
            IScheduler* scheduler,
            std::any&& value)
            : m_scheduler(scheduler),
            m_value(move(value))
        {}

    public:
        bool await_ready() noexcept
        {
            return m_scheduler->await_ready(
                &m_value);
        }

        void await_suspend(std::coroutine_handle<> awaitingCoroutine) noexcept
        {
            m_scheduler->await_suspend(
                &m_value,
                awaitingCoroutine);
        }

        void await_resume()
        {
            return m_scheduler->await_resume(
                &m_value);
        }
    };

    virtual std::any create_schedule_operation_value(
    ) = 0;

    virtual bool await_ready(
        std::any* scheduleOperationValue
    ) noexcept = 0;

    virtual void await_suspend(
        std::any* scheduleOperationValue,
        std::coroutine_handle<> awaitingCoroutine
    ) noexcept = 0;

    virtual void await_resume(
        std::any* scheduleOperationValue
    ) noexcept = 0;

    schedule_operation schedule()
    {
        return schedule_operation(
            this,
            create_schedule_operation_value());
    }

    schedule_operation operator co_await()
    {
        return schedule();
    }
};

template<
    typename TScheduler
> class DefaultScheduler
    :
    public IScheduler
{
    TScheduler m_scheduler;
    typedef decltype(m_scheduler.schedule()) underlying_schedule_operation;

public:
    template<
        typename ... TArgs
    >
        DefaultScheduler(
            TArgs&& ... args
        ) : m_scheduler(std::forward<TArgs>(args)...)
    {}

    virtual std::any create_schedule_operation_value(
    ) override
    {
        return m_scheduler.schedule();
    }

    virtual bool await_ready(
        std::any* value
    ) noexcept override
    {
        return std::any_cast<underlying_schedule_operation>(value)->await_ready();
    }

    virtual void await_suspend(
        std::any* value,
        std::coroutine_handle<> awaitingCoroutine
    ) noexcept override
    {
        return std::any_cast<underlying_schedule_operation>(value)->await_suspend(
            awaitingCoroutine);
    }

    virtual void await_resume(
        std::any* value
    ) noexcept override
    {
        return std::any_cast<underlying_schedule_operation>(value)->await_resume();
    }
};

class IJoinable
{
public:
    virtual cppcoro::shared_task<> join() = 0;
};

class BackgroundWorkerImpl
    :
    virtual public IJoinable
{
    friend class BackgroundWorker;
    template<
        typename T
    > friend class BaseBackgoundWorker;

    class Joiner;

    class Worker
        :
        public std::enable_shared_from_this<Worker>
    {
        friend class Joiner;

        cppcoro::async_manual_reset_event m_completed;
        cppcoro::async_scope m_workerAsyncScope;
        cppcoro::shared_task<> m_joinTask;
        
        template<
            typename TAwaitable,
            typename... THolderVariables
        > cppcoro::task<> Run(
            std::decay_t<TAwaitable> awaitable,
            std::decay_t<THolderVariables>... holderVariables
        )
        {
            co_await awaitable;
        }

        cppcoro::shared_task<> waitForCompleteAndJoin()
        {
            co_await m_completed;
            co_await m_workerAsyncScope.join();
        }

    public:
        Worker()
        {
            m_joinTask = waitForCompleteAndJoin();
        }

        ~Worker()
        {
            complete();

            // This will never wait.
            // Unfortunately, we can't quite assert() it, because
            // we don't eagerly start it anywhere else.
            cppcoro::sync_wait(
                m_joinTask);
        }

        template<
            typename TAwaitable,
            typename... THolderVariables
        > void spawn(
            TAwaitable&& awaitable,
            THolderVariables&&... holderVariables
        )
        {
            m_workerAsyncScope.spawn(
                Run<TAwaitable, THolderVariables...>(
                    std::forward<TAwaitable>(awaitable),
                    std::forward<THolderVariables>(holderVariables)...
                    )
            );
        }

    protected:
        void complete()
        {
            m_completed.set();
        }

    public:
        auto join()
        {
            return m_joinTask;
        }
    };

    class Joiner
    {
        std::shared_ptr<Worker> m_worker;
    public:
        Joiner(
            std::shared_ptr<Worker> worker
        ) : m_worker(
            worker)
        {}

        ~Joiner()
        {
            m_worker->complete();
        }

        const std::shared_ptr<Worker>& worker()
        {
            return m_worker;
        }
    };

    std::shared_ptr<Joiner> m_joiner;

    template<
        typename TAwaitable,
        typename... THolderVariables
    > void spawn_no_holders(
        TAwaitable&& awaitable,
        THolderVariables&&... holderVariables
    )
    {
        m_joiner->worker()->spawn(
            std::forward<TAwaitable>(awaitable),
            std::forward<THolderVariables>(holderVariables)...);
    }

public:
    BackgroundWorkerImpl()
        :
        m_joiner(
            std::make_shared<Joiner>(
                std::make_shared<Worker>()))
    {}

    cppcoro::shared_task<> join() override
    {
        return m_joiner->worker()->join();
    }
};

// A BackgroundWorker allows scheduling eager tasks while keeping alive
// any objects needed to execute the task, including the dervied class of BaseBackgroundWorker
// itself, while also providing a facility to wait for all tasks to complete.
template<
    typename TDerived
> class BaseBackgoundWorker
    :
    public BackgroundWorkerImpl
{
protected:
    template<
        typename TAwaitable,
        typename... THolderVariables
    > void spawn(
        TAwaitable&& awaitable,
        THolderVariables&&... holderVariables
    )
    {
        BackgroundWorkerImpl::spawn_no_holders(
            std::forward<TAwaitable>(awaitable),
            // The shared_from_this implicitly keeps alive the m_joiner,
            // which implicitly keeps alive m_joiner->m_worker
            static_cast<const TDerived*>(this)->shared_from_this(),
            std::forward<THolderVariables>(holderVariables)...);
    }
};

// A BackgroundWorker allows scheduling eager tasks while keeping alive
// any objects needed to execute the task, including the BackgroundWorker
// itself, while also providing a facility to wait for all tasks to complete.
class BackgroundWorker
    :
    public BackgroundWorkerImpl
{
public:

    template<
        typename TAwaitable,
        typename... THolderVariables
    > void spawn(
        TAwaitable&& awaitable,
        THolderVariables&&... holderVariables
    )
    {
        BackgroundWorkerImpl::spawn_no_holders(
            std::forward<TAwaitable>(awaitable),
            m_joiner->worker(),
            std::forward<THolderVariables>(holderVariables)...);
    }
};
}
