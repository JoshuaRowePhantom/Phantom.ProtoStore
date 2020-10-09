#include "StandardIncludes.h"
#include "Phantom.System/async_reader_writer_lock.h"
#include <cppcoro/async_manual_reset_event.hpp>
#include <cppcoro/async_scope.hpp>

using namespace cppcoro;

namespace Phantom
{

TEST(async_reader_writer_lock_tests, can_acquire_multiple_reads)
{
    run_async([]() -> task<>
    {
        async_reader_writer_lock lock;

        auto lock1 = co_await lock.reader().scoped_lock_async();
        auto lock2 = co_await lock.reader().scoped_lock_async();
    });
}

TEST(async_reader_writer_lock_tests, cannot_acquire_more_reads_after_write_lock_requested)
{
    run_async([]() -> task<>
    {
        async_reader_writer_lock lock;
        async_scope runner;

        co_await lock.reader().lock_async();
        co_await lock.reader().lock_async();

        async_manual_reset_event writeLockAcquired;
        async_manual_reset_event releaseWriteLock;

        auto writeLockLambda = [&]() -> task<>
        {
            co_await lock.writer().lock_async();
            writeLockAcquired.set();
            co_await releaseWriteLock;
            lock.writer().unlock();
        };

        async_manual_reset_event readLockAcquired;

        auto readLockLambda = [&]() -> task<>
        {
            co_await lock.reader().lock_async();
            readLockAcquired.set();
        };

        runner.spawn(writeLockLambda());
        runner.spawn(readLockLambda());

        EXPECT_EQ(false, writeLockAcquired.is_set());
        EXPECT_EQ(false, readLockAcquired.is_set());

        lock.reader().unlock();

        EXPECT_EQ(false, writeLockAcquired.is_set());
        EXPECT_EQ(false, readLockAcquired.is_set());

        lock.reader().unlock();

        co_await writeLockAcquired;
        EXPECT_EQ(false, readLockAcquired.is_set());

        releaseWriteLock.set();
        co_await readLockAcquired;

        co_await runner.join();
    });
}

TEST(async_reader_writer_lock_tests, cannot_acquire_more_writes_after_write_lock_requested)
{
    run_async([]() -> task<>
    {
        async_reader_writer_lock lock;
        async_scope runner;

        async_manual_reset_event writeLockAcquired1;
        async_manual_reset_event releaseWriteLock1;

        auto writeLockLambda1 = [&]() -> task<>
        {
            auto writeLock = co_await lock.writer().scoped_lock_async();
            writeLockAcquired1.set();
            co_await releaseWriteLock1;
        };

        async_manual_reset_event writeLockAcquired2;
        async_manual_reset_event releaseWriteLock2;

        auto writeLockLambda2 = [&]() -> task<>
        {
            auto writeLock = co_await lock.writer().scoped_lock_async();
            writeLockAcquired2.set();
            co_await releaseWriteLock2;
        };

        runner.spawn(writeLockLambda1());
        runner.spawn(writeLockLambda2());

        co_await writeLockAcquired1;
        EXPECT_EQ(false, writeLockAcquired2.is_set());

        releaseWriteLock1.set();

        co_await writeLockAcquired2;

        releaseWriteLock2.set();

        co_await runner.join();
    });
}

TEST(async_reader_writer_lock_tests, can_try_lock)
{
    run_async([]() -> task<>
    {
        async_reader_writer_lock lock;
        EXPECT_EQ(true, lock.reader().try_lock());
        EXPECT_EQ(true, lock.reader().try_lock());
        EXPECT_EQ(false, lock.writer().try_lock());
        lock.reader().unlock();
        EXPECT_EQ(false, lock.writer().try_lock());
        lock.reader().unlock();
        EXPECT_EQ(true, lock.writer().try_lock());
        EXPECT_EQ(false, lock.writer().try_lock());
        EXPECT_EQ(false, lock.reader().try_lock());
        lock.writer().unlock();
        EXPECT_EQ(true, lock.writer().try_lock());
        EXPECT_EQ(false, lock.writer().try_lock());
        lock.writer().unlock();
        EXPECT_EQ(true, lock.reader().try_lock());
        lock.reader().unlock();

        co_return;
    });
}

TEST(async_reader_writer_lock_tests, waiting_writer_lock_prevents_try_read_lock)
{
    run_async([]() -> task<>
    {
        async_reader_writer_lock lock;
        
        EXPECT_EQ(true, lock.reader().try_lock());
        async_scope runner;

        auto writeLockLambda1 = [&]() -> task<>
        {
            auto writeLock = co_await lock.writer().scoped_lock_async();
        };

        runner.spawn(writeLockLambda1());

        EXPECT_EQ(false, lock.reader().try_lock());
        lock.reader().unlock();

        co_await runner.join();
    });
}

}