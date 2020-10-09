#pragma once

namespace Phantom
{

template<typename TLock>
[[nodiscard]]
class async_scoped_lock
{
    TLock* m_object;

public:
    async_scoped_lock(
        TLock& object,
        std::adopt_lock_t
    )
        : m_object(&object)
    {
    }

    async_scoped_lock(
        TLock& object,
        std::try_to_lock_t
    )
        : m_object(nullptr)
    {
        if (object.try_lock())
        {
            m_object = &object;
        }
    }

    async_scoped_lock(
        async_scoped_lock&& other
    ) : m_object(other.m_object)
    {
        other.m_object = nullptr;
    }

    async_scoped_lock(
        const async_scoped_lock&
    ) = delete;

    async_scoped_lock& operator=(
        const async_scoped_lock&
        ) = delete;

    async_scoped_lock& operator=(
        async_scoped_lock&& other
        )
    {
        if (m_object)
        {
            m_object->unlock();
        }

        m_object = other.m_object;
        other.m_object = nullptr;

        return *this;
    }

    explicit operator bool() const
    {
        return m_object;
    }

    ~async_scoped_lock()
    {
        if (m_object)
        {
            m_object->unlock();
        }
    }
};

template<
    typename TLock,
    typename TAwaitable
> 
class async_scoped_lock_operation
{
    TLock& m_lock;
    TAwaitable m_awaitable;

public:
    async_scoped_lock_operation(
        TLock& lock,
        TAwaitable awaitable
    ) :
        m_lock(lock),
        m_awaitable(awaitable)
    {}

    bool await_ready() noexcept
    {
        return m_awaitable.await_ready();
    }

    bool await_suspend(
        std::coroutine_handle<> awaiter
    ) noexcept
    {
        return m_awaitable.await_suspend(
            awaiter);
    }

    [[nodiscard]]
    async_scoped_lock<TLock> await_resume() const noexcept
    {
        return async_scoped_lock<TLock>(
            m_lock,
            std::adopt_lock);
    }
};
}
