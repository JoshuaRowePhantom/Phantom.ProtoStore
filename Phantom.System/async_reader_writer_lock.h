#pragma once

#include <atomic>
#include <cppcoro/async_auto_reset_event.hpp>
#include <cppcoro/async_manual_reset_event.hpp>
#include <cppcoro/task.hpp>
#include <mutex> // for std::adopt_lock_t

namespace Phantom
{

class async_reader_writer_lock;

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

class async_reader_writer_lock;

class async_linked_reader_lock
{
    async_reader_writer_lock& m_asyncReaderWriterLock;
public:
    async_linked_reader_lock(
        async_reader_writer_lock& asyncReaderWriterLock
    )
        : m_asyncReaderWriterLock(asyncReaderWriterLock)
    {}

    typedef async_scoped_lock<async_linked_reader_lock> scoped_lock;
    
    [[nodiscard]]
    bool try_lock() noexcept;

    [[nodiscard]]
    scoped_lock scoped_try_lock() noexcept;
    
    [[nodiscard]]
    cppcoro::task<> lock_async() noexcept;
    
    [[nodiscard]]
    cppcoro::task<scoped_lock> scoped_lock_async() noexcept;
    
    void unlock() noexcept;
};

class async_linked_writer_lock
{
    async_reader_writer_lock& m_asyncReaderWriterLock;
public:
    async_linked_writer_lock(
        async_reader_writer_lock& asyncReaderWriterLock
    )
        : m_asyncReaderWriterLock(asyncReaderWriterLock)
    {}

    typedef async_scoped_lock<async_linked_writer_lock> scoped_lock;

    [[nodiscard]]
    bool try_lock() noexcept;

    [[nodiscard]]
    scoped_lock scoped_try_lock() noexcept;

    [[nodiscard]]
    cppcoro::task<> lock_async() noexcept;

    [[nodiscard]]
    cppcoro::task<scoped_lock> scoped_lock_async() noexcept;
    void unlock() noexcept;
};

class async_reader_writer_lock
{
    std::atomic<uint_fast64_t> m_readerCount;
    cppcoro::async_manual_reset_event m_readerSignal;
    cppcoro::async_auto_reset_event m_writerSignal;

    friend class async_linked_writer_lock;
    friend class async_linked_reader_lock;
public:
    async_reader_writer_lock()
        : m_readerCount(0)
    {
        m_readerSignal.set();
        m_writerSignal.set();
    }

    async_linked_reader_lock reader()
    {
        return async_linked_reader_lock(*this);
    }

    async_linked_writer_lock writer()
    {
        return async_linked_writer_lock(*this);
    }
};

cppcoro::task<> async_linked_reader_lock::lock_async() noexcept
{
    do
    {
        co_await m_asyncReaderWriterLock.m_readerSignal;
    } while (!try_lock());
}

void async_linked_reader_lock::unlock() noexcept
{
    if (m_asyncReaderWriterLock.m_readerCount.fetch_sub(1) == 1)
    {
        m_asyncReaderWriterLock.m_writerSignal.set();
    }
}

cppcoro::task<async_linked_reader_lock::scoped_lock> async_linked_reader_lock::scoped_lock_async() noexcept
{
    co_await lock_async();
    co_return async_scoped_lock<async_linked_reader_lock>(
        *this,
        std::adopt_lock);
}

bool async_linked_reader_lock::try_lock() noexcept
{
    auto readerCount = m_asyncReaderWriterLock.m_readerCount.load(
        std::memory_order_acquire);

    if (m_asyncReaderWriterLock.m_readerSignal.is_set()
        &&
        readerCount >= 0
        &&
        m_asyncReaderWriterLock.m_readerCount.compare_exchange_strong(
            readerCount,
            readerCount + 1,
            std::memory_order_acq_rel))
    {
        m_asyncReaderWriterLock.m_writerSignal.reset();
        return true;
    }

    return false;
}

async_linked_reader_lock::scoped_lock async_linked_reader_lock::scoped_try_lock() noexcept
{
    return scoped_lock(
        *this,
        std::try_to_lock);
}

bool async_linked_writer_lock::try_lock() noexcept
{
    size_t expectedReaderCount = 0;
    if (m_asyncReaderWriterLock.m_readerCount.compare_exchange_strong(
        expectedReaderCount,
        -1,
        std::memory_order_acq_rel))
    {
        m_asyncReaderWriterLock.m_readerSignal.reset();
        m_asyncReaderWriterLock.m_writerSignal.reset();
        return true;
    }
    return false;
}

async_linked_writer_lock::scoped_lock async_linked_writer_lock::scoped_try_lock() noexcept
{
    return scoped_lock(
        *this,
        std::try_to_lock);
}

cppcoro::task<> async_linked_writer_lock::lock_async() noexcept
{
    m_asyncReaderWriterLock.m_readerSignal.reset();
    do
    {
        co_await m_asyncReaderWriterLock.m_writerSignal;
    } while (!try_lock());
}

void async_linked_writer_lock::unlock() noexcept
{
    m_asyncReaderWriterLock.m_readerCount.store(
        0,
        std::memory_order_release
    );

    m_asyncReaderWriterLock.m_writerSignal.set();
    m_asyncReaderWriterLock.m_readerSignal.set();
}

cppcoro::task<async_linked_writer_lock::scoped_lock> async_linked_writer_lock::scoped_lock_async() noexcept
{
    co_await lock_async();
    co_return async_scoped_lock<async_linked_writer_lock>(
        *this,
        std::adopt_lock);
}

}