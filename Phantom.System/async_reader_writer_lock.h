#pragma once

#include <atomic>
#include <cppcoro/async_auto_reset_event.hpp>
#include <cppcoro/async_manual_reset_event.hpp>
#include <cppcoro/task.hpp>
#include <mutex> // for std::adopt_lock_t
#include "async_scoped_lock.h"

namespace Phantom
{
class async_reader_writer_lock;

class async_linked_reader_lock
{
    async_reader_writer_lock& m_asyncReaderWriterLock;

public:
    struct lock_async_operation; 
    
    async_linked_reader_lock(
        async_reader_writer_lock& asyncReaderWriterLock
    );

    typedef async_scoped_lock<async_linked_reader_lock> scoped_lock;
    typedef async_scoped_lock_operation<async_linked_reader_lock, lock_async_operation> scoped_lock_operation;

    [[nodiscard]]
    bool try_lock() noexcept;

    [[nodiscard]]
    scoped_lock scoped_try_lock() noexcept;
    
    [[nodiscard]]
    lock_async_operation lock_async() noexcept;
    
    [[nodiscard]]
    scoped_lock_operation scoped_lock_async() noexcept;
    
    void unlock() noexcept;
};

class async_linked_writer_lock
{
    async_reader_writer_lock& m_asyncReaderWriterLock;
public:
    struct lock_async_operation;

    async_linked_writer_lock(
        async_reader_writer_lock& asyncReaderWriterLock
    );

    typedef async_scoped_lock<async_linked_writer_lock> scoped_lock;
    typedef async_scoped_lock_operation<async_linked_writer_lock, lock_async_operation> scoped_lock_operation;

    bool has_owner() const noexcept;
    bool has_waiter() const noexcept;

    [[nodiscard]]
    bool try_lock() noexcept;

    [[nodiscard]]
    scoped_lock scoped_try_lock() noexcept;

    [[nodiscard]]
    lock_async_operation lock_async() noexcept;

    [[nodiscard]]
    scoped_lock_operation scoped_lock_async() noexcept;
    void unlock() noexcept;

};

class async_reader_writer_lock
{
    std::atomic<int_fast64_t> m_readerCount;

    std::atomic<async_linked_reader_lock::lock_async_operation*> m_waitingReaders;
    std::atomic<async_linked_writer_lock::lock_async_operation*> m_waitingWriters;

    void try_signal_waiters();

    friend class async_linked_writer_lock;
    friend class async_linked_reader_lock;
public:
    async_reader_writer_lock();
    async_linked_reader_lock reader();
    async_linked_writer_lock writer();
};

class async_linked_reader_lock::lock_async_operation
{
    async_linked_reader_lock m_readerLock;
    lock_async_operation* m_next;
    std::experimental::coroutine_handle<> m_awaiter;

    friend class async_reader_writer_lock;
public:
    explicit lock_async_operation(
        const async_linked_reader_lock& readerLock
    );

    bool await_ready() noexcept;

    bool await_suspend(std::experimental::coroutine_handle<> awaiter) noexcept;

    void await_resume() const noexcept;
};

class async_linked_writer_lock::lock_async_operation
{
    async_linked_writer_lock m_writerLock;
    lock_async_operation* m_next;
    std::experimental::coroutine_handle<> m_awaiter;

    friend class async_reader_writer_lock;
public:
    explicit lock_async_operation(
        const async_linked_writer_lock& writerLock
    );

    bool await_ready() noexcept;

    bool await_suspend(std::experimental::coroutine_handle<> awaiter) noexcept;

    void await_resume() const noexcept;
};

}