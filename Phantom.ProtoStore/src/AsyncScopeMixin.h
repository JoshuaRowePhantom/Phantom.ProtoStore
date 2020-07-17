#pragma once

#include <cppcoro/async_scope.hpp>
#include <cppcoro/shared_task.hpp>
#include <cppcoro/sync_wait.hpp>

namespace Phantom::ProtoStore
{

// Base class for classes that need an async_scope.
class AsyncScopeMixin
    :
    public virtual IJoinable
{
    // These are declared in this order intenionally.
protected:
    cppcoro::async_scope m_asyncScope;
private:
    cppcoro::shared_task<> m_joinTask;

    shared_task<> InternalJoinTask()
    {
        co_await m_asyncScope.join();
    }

protected:

    AsyncScopeMixin()
        :
        m_joinTask(InternalJoinTask())
    {
    }

    ~AsyncScopeMixin()
    {
        cppcoro::sync_wait(
            m_joinTask);
    }

public:
    virtual cppcoro::task<> Join() override
    {
        co_await m_joinTask;
    }
};

}