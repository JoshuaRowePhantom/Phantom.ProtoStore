#pragma once

#include <atomic>
#include <cppcoro/async_auto_reset_event.hpp>
#include <cppcoro/async_manual_reset_event.hpp>
#include <cppcoro/task.hpp>
#include <mutex> // for std::adopt_lock_t

namespace Phantom
{

class async_reader_writer_lock;

namespace detail
{
template<void (async_reader_writer_lock::* UnlockFunction)()>
class async_reader_writer_base_lock
{
    async_reader_writer_lock* m_object;

public:
    async_reader_writer_base_lock(
        async_reader_writer_lock& object,
        std::adopt_lock_t
    )
        :
        m_object(&object)
    {}

    ~async_reader_writer_base_lock()
    {
        if (m_object)
        {
            (m_object->*UnlockFunction)();
        }
    }

    async_reader_writer_base_lock(
        async_reader_writer_base_lock&& other
    )
        :
        m_object(other.m_object)
    {
        other.m_object = nullptr;
    }

    async_reader_writer_base_lock& operator=(
        const async_reader_writer_base_lock& other
        ) = delete;

    async_reader_writer_base_lock& operator=(
        async_reader_writer_base_lock&& other
        )
    {
        m_object = other.m_object;
        other.m_object = nullptr;
    }
};
}

class async_reader_writer_lock
{
    std::atomic<uint_fast64_t> m_readerCount;
    cppcoro::async_manual_reset_event m_readerSignal;
    cppcoro::async_auto_reset_event m_writerSignal;

public:
    async_reader_writer_lock()
        : m_readerCount(0)
    {
        m_readerSignal.set();
        m_writerSignal.set();
    }

    async_reader_writer_lock(
        const async_reader_writer_lock&
    ) = delete;

    async_reader_writer_lock(
        async_reader_writer_lock&&
    ) = delete;

    async_reader_writer_lock& operator=(
        const async_reader_writer_lock&
    ) = delete;

    async_reader_writer_lock& operator=(
        async_reader_writer_lock&&
        ) = delete;

    cppcoro::task<> nonrecursive_lock_read_async()
    {
        while(true)
        {
            co_await m_readerSignal;

            auto readerCount = m_readerCount.load(
                std::memory_order_acquire);

            if (m_readerCount.compare_exchange_strong(
                readerCount,
                readerCount + 1,
                std::memory_order_acq_rel))
            {
                m_writerSignal.reset();
                co_return;
            }
        }
    }

    void unlock_read()
    {
        if (m_readerCount.fetch_sub(1) == 1)
        {
            m_writerSignal.set();
        }
    }

    cppcoro::task<> nonrecursive_lock_write_async()
    {
        m_readerSignal.reset();

        size_t expectedReaderCount;
        do
        {
            expectedReaderCount = 0;
            co_await m_writerSignal;
        } while (!m_readerCount.compare_exchange_strong(
            expectedReaderCount,
            -1,
            std::memory_order_acq_rel));
    }

    void unlock_write()
    {
        m_readerCount.store(
            0,
            std::memory_order_release
        );

        m_readerSignal.set();
        m_writerSignal.set();
    }

    typedef detail::async_reader_writer_base_lock<&async_reader_writer_lock::unlock_read> async_reader_writer_read_lock;
    typedef detail::async_reader_writer_base_lock<&async_reader_writer_lock::unlock_write> async_reader_writer_write_lock;

    cppcoro::task<async_reader_writer_read_lock> scoped_nonrecursive_lock_read_async()
    {
        co_await nonrecursive_lock_read_async();
        co_return async_reader_writer_read_lock(
            *this,
            std::adopt_lock);
    }

    cppcoro::task<async_reader_writer_write_lock> scoped_nonrecursive_lock_write_async()
    {
        co_await nonrecursive_lock_write_async();
        co_return async_reader_writer_write_lock(
            *this,
            std::adopt_lock);
    }
};

}