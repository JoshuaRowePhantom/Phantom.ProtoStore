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

}
