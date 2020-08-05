#pragma once

#include <cppcoro/async_scope.hpp>
#include <cppcoro/shared_task.hpp>
#include <cppcoro/sync_wait.hpp>
#include <cppcoro/cancellation_source.hpp>
#include <cppcoro/cancellation_token.hpp>

namespace Phantom::ProtoStore
{

// Base class for classes that need an async_scope.
// The proper use is to derived _public_ from this class,
// and have each derived class's destructor call SyncDestroy().
// This ensures that derived class's
// member variables are not destroyed until
// after the Join() operation completes.
// Derived classes can use the cancellation tokens
// to receive notifications to cancel.
class AsyncScopeMixin
    :
    public virtual IJoinable
{
    // These are declared in this order intentionally.
protected:
    template<
        typename TAwaitable
    > void spawn(
        TAwaitable&& awaitable
    )
    {
        m_asyncScope.spawn(
            std::forward<TAwaitable>(awaitable)
        );
    }

private:
    cppcoro::async_scope m_asyncScope;
    cppcoro::shared_task<> m_joinTask;
    cppcoro::cancellation_source m_destructorCalledCancellationSource;
    cppcoro::cancellation_token m_destructorCalledCancellationToken;
    cppcoro::cancellation_source m_joinCalledCancellationSource;
    cppcoro::cancellation_token m_joinCalledCancellationToken;

    shared_task<> InternalJoinTask()
    {
        co_await m_asyncScope.join();
    }

protected:

    AsyncScopeMixin()
        :
        m_joinTask(InternalJoinTask()),
        m_destructorCalledCancellationToken(m_destructorCalledCancellationSource.token()),
        m_joinCalledCancellationToken(m_joinCalledCancellationSource.token())
    {
    }

    void SyncDestroy()
    {
        m_destructorCalledCancellationSource.request_cancellation();

        cppcoro::sync_wait(
            m_joinTask);
    }

    const cppcoro::cancellation_token& GetDestroyCancellationToken()
    {
        return m_destructorCalledCancellationToken;
    }

    const cppcoro::cancellation_token& GetJoinCalledCancellationToken()
    {
        return m_joinCalledCancellationToken;
    }

    ~AsyncScopeMixin()
    {
        SyncDestroy();
    }

public:
    shared_task<> Joined() const
    {
        return m_joinTask;
    }

    virtual cppcoro::task<> Join() override
    {
        m_joinCalledCancellationSource.request_cancellation();
        co_await m_joinTask;
    }
};

}