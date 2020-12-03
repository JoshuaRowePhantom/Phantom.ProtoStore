#pragma once

#include "Memory.h"
#include <any>
#include <coroutine>
#include <cppcoro/async_scope.hpp>
#include <cppcoro/shared_task.hpp>
#include <cppcoro/sync_wait.hpp>

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

class BackgroundWorker
{
    class WorkerImplementation
        :
        public std::enable_shared_from_this<WorkerImplementation>
    {
        cppcoro::async_scope m_asyncScope;
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

    public:
        WorkerImplementation()
        {
            m_joinTask = cppcoro::make_shared_task(
                m_asyncScope.join());
        }

        ~WorkerImplementation()
        {
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
            m_asyncScope.spawn(
                Run<TAwaitable>(
                    shared_from_this(),
                    std::forward<TAwaitable>(awaitable),
                    std::forward<THolderVariables>(holderVariables)...
                    )
            );
        }

        auto join()
        {
            return m_joinTask;
        }
    };

    std::shared_ptr<WorkerImplementation> m_workerImplementation;

public:
    BackgroundWorker()
        :
        m_workerImplementation(
            std::make_shared<WorkerImplementation>())
    {}

    template<
        typename TAwaitable,
        typename... THolderVariables
    > void spawn(
        TAwaitable&& awaitable,
        THolderVariables&&... holderVariables
    )
    {
        m_workerImplementation->m_asyncScope.spawn(
            std::forward<TAwaitable>(awaitable),
            std::forward<THolderVariables>(holderVariables)...);
    }
};

}
