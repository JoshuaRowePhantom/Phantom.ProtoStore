#include "async_reader_writer_lock.h"

namespace Phantom
{

async_linked_reader_lock::async_linked_reader_lock(
    async_reader_writer_lock& asyncReaderWriterLock
)
    : m_asyncReaderWriterLock(asyncReaderWriterLock)
{}

async_linked_writer_lock::async_linked_writer_lock(
    async_reader_writer_lock& asyncReaderWriterLock
)
    : m_asyncReaderWriterLock(asyncReaderWriterLock)
{}


async_reader_writer_lock::async_reader_writer_lock()
    : m_readerCount(0),
    m_waitingReaders(nullptr),
    m_waitingWriters(nullptr)
{
}

async_linked_reader_lock async_reader_writer_lock::reader()
{
    return async_linked_reader_lock(*this);
}

async_linked_writer_lock async_reader_writer_lock::writer()
{
    return async_linked_writer_lock(*this);
}

async_linked_reader_lock::lock_async_operation::lock_async_operation(
    const async_linked_reader_lock& readerLock
) :
    m_readerLock(readerLock)
{
}

bool async_linked_reader_lock::lock_async_operation::await_ready(
) noexcept
{
    return m_readerLock.try_lock();
}

bool async_linked_reader_lock::lock_async_operation::await_suspend(
    std::coroutine_handle<> awaiter
) noexcept
{
    m_awaiter = awaiter;

    do
    {
        if (m_readerLock.try_lock())
        {
            return false;
        }

        auto expectedWaiter = m_readerLock.m_asyncReaderWriterLock.m_waitingReaders.load(
            std::memory_order_relaxed);

        m_next = expectedWaiter;

        if (m_readerLock.m_asyncReaderWriterLock.m_waitingReaders.compare_exchange_weak(
            expectedWaiter,
            this,
            std::memory_order_acq_rel))
        {
            break;
        }
    } while (true);

    m_readerLock.m_asyncReaderWriterLock.try_signal_waiters();
    return true;
}

void async_linked_reader_lock::lock_async_operation::await_resume() const noexcept {}

async_linked_reader_lock::lock_async_operation async_linked_reader_lock::lock_async() noexcept
{
    return lock_async_operation(*this);
}

void async_linked_reader_lock::unlock() noexcept
{
    if (m_asyncReaderWriterLock.m_readerCount.fetch_sub(1) == 1)
    {
        m_asyncReaderWriterLock.try_signal_waiters();
    }
}

async_linked_reader_lock::scoped_lock_operation async_linked_reader_lock::scoped_lock_async() noexcept
{
    return scoped_lock_operation(
        *this,
        lock_async());
}

bool async_linked_reader_lock::try_lock() noexcept
{
    auto readerCount = m_asyncReaderWriterLock.m_readerCount.load(
        std::memory_order_acquire);

    if (!m_asyncReaderWriterLock.m_waitingWriters.load(std::memory_order_relaxed)
        &&
        readerCount >= 0
        &&
        m_asyncReaderWriterLock.m_readerCount.compare_exchange_strong(
            readerCount,
            readerCount + 1,
            std::memory_order_acq_rel))
    {
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

async_linked_writer_lock::lock_async_operation::lock_async_operation(
    const async_linked_writer_lock& writerLock
) :
    m_writerLock(writerLock)
{
}

bool async_linked_writer_lock::lock_async_operation::await_ready() noexcept
{
    return m_writerLock.try_lock();
}

bool async_linked_writer_lock::lock_async_operation::await_suspend(
    std::coroutine_handle<> awaiter
) noexcept
{
    m_awaiter = awaiter;

    do
    {
        if (m_writerLock.try_lock())
        {
            return false;
        }

        auto expectedWaiter = m_writerLock.m_asyncReaderWriterLock.m_waitingWriters.load(
            std::memory_order_relaxed);

        m_next = expectedWaiter;

        if (m_writerLock.m_asyncReaderWriterLock.m_waitingWriters.compare_exchange_weak(
            expectedWaiter,
            this,
            std::memory_order_acq_rel))
        {
            break;
        }
    } while (true);

    m_writerLock.m_asyncReaderWriterLock.try_signal_waiters();
    return true;
}

bool async_linked_writer_lock::has_owner() const noexcept
{
    return m_asyncReaderWriterLock.m_readerCount.load(
        std::memory_order_acquire
    ) == -1;
}

bool async_linked_writer_lock::has_waiter() const noexcept
{
    return m_asyncReaderWriterLock.m_waitingWriters.load(
        std::memory_order_acquire);
}

void async_linked_writer_lock::lock_async_operation::await_resume(
) const noexcept {}

bool async_linked_writer_lock::try_lock() noexcept
{
    int_fast64_t expectedReaderCount = 0;
    if (m_asyncReaderWriterLock.m_readerCount.compare_exchange_strong(
        expectedReaderCount,
        -1,
        std::memory_order_acq_rel))
    {
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

async_linked_writer_lock::lock_async_operation async_linked_writer_lock::lock_async() noexcept
{
    return lock_async_operation(*this);
}

void async_linked_writer_lock::unlock() noexcept
{
    m_asyncReaderWriterLock.m_readerCount.store(
        0,
        std::memory_order_release
    );

    m_asyncReaderWriterLock.try_signal_waiters();
}

async_linked_writer_lock::scoped_lock_operation async_linked_writer_lock::scoped_lock_async() noexcept
{
    return scoped_lock_operation(
        *this,
        lock_async());
}

void async_reader_writer_lock::try_signal_waiters()
{
    auto waitingWriter = m_waitingWriters.load(
        std::memory_order_acquire);

    auto readerCount = m_readerCount.load(
        std::memory_order_relaxed);

    if (readerCount == -1)
    {
        // There is a writer holding the lock, so nobody can wake up.
        return;
    }

    if (waitingWriter
        && readerCount == 0
        && m_readerCount.compare_exchange_strong(
            readerCount,
            -1))
    {
        // We acquired the lock for -a- writer,
        // but the list may have changed.
        while (waitingWriter)
        {
            auto nextWriter = waitingWriter->m_next;

            if (m_waitingWriters.compare_exchange_strong(
                waitingWriter,
                nextWriter,
                std::memory_order_acq_rel
            ))
            {
                waitingWriter->m_awaiter.resume();
                return;
            }
        }

        // If we reached here, there was actually no writer to wake up.
        m_readerCount.store(
            0,
            std::memory_order_release);
    }

    if (waitingWriter)
    {
        // Because there was a waiting writer,
        // don't try to signal readers.
        return;
    }

    // If we reached here, try to signal each waiting reader.
    auto waitingReader = m_waitingReaders.load(
        std::memory_order_acquire);

    // See if we can release the readers by artifically increasing
    // the read count to at least 1.
    if (!waitingReader
        ||
        !m_readerCount.compare_exchange_strong(
            readerCount,
            readerCount + 1))
    {
        return;
    }

    // Pull all the readers out all at once.
    while (!m_waitingReaders.compare_exchange_strong(
        waitingReader,
        nullptr,
        std::memory_order_acq_rel))
    { }

    // Now signal all the readers
    while (waitingReader)
    {
        // We can use fetch_add to increase the reader count
        // because we artifically inflated the reader count by 1 earlier.
        m_readerCount.fetch_add(1);

        auto nextReader = waitingReader->m_next;
        waitingReader->m_awaiter.resume();
        waitingReader = nextReader;
    }

    // We artifically increased the reader count,
    // so now we have to decrement it.
    m_readerCount.fetch_sub(1);
}

}