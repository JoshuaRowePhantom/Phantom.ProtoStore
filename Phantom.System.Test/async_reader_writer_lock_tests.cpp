#include "StandardIncludes.h"
#include "async_test.h"
#include "Phantom.Coroutines/inline_scheduler.h"
#include "Phantom.System/async_reader_writer_lock.h"

namespace Phantom
{

ASYNC_TEST(execute_conditional_read_unlikely_write_operation_tests, lock_is_acquired_in_appropriate_mode)
{
    for (auto expectedCallCount = 1; expectedCallCount < 10; ++expectedCallCount)
    {
        Phantom::async_reader_writer_lock reader_writer_lock;
        Phantom::Coroutines::inline_scheduler scheduler;
        int callCount = 0;

        co_await execute_conditional_read_unlikely_write_operation(
            reader_writer_lock,
            scheduler,
            [&](bool hasWriteLock) -> cppcoro::task<bool>
        {
            EXPECT_EQ(hasWriteLock, callCount % 2 == 1);

            if (++callCount == expectedCallCount)
            {
                co_return true;
            }

            if (callCount > expectedCallCount)
            {
                throw "Invalid call count";
            }

            if (hasWriteLock)
            {
                EXPECT_EQ(false, reader_writer_lock.reader().try_lock());
            }
            else
            {
                EXPECT_EQ(false, reader_writer_lock.writer().try_lock());
            }

            co_return false;
        });

        EXPECT_EQ(expectedCallCount, callCount);
    }
}

ASYNC_TEST(execute_conditional_read_unlikely_write_operation_tests, two_argument_version_doesnt_call_writer_if_reader_returns_true)
{
    Phantom::async_reader_writer_lock reader_writer_lock;
    Phantom::Coroutines::inline_scheduler scheduler;
    
    bool readerCalled = false;

    co_await execute_conditional_read_unlikely_write_operation(
        reader_writer_lock,
        scheduler,
        [&](bool hasWriteLock) -> cppcoro::task<bool>
    {
        readerCalled = true;
        EXPECT_EQ(false, hasWriteLock);
        co_return true;
    },
        [&]() -> cppcoro::task<>
    {
        EXPECT_TRUE(false);
        co_return;
    }
    );

    EXPECT_EQ(true, readerCalled);
}

ASYNC_TEST(execute_conditional_read_unlikely_write_operation_tests, two_argument_version_does_call_reader_with_write_lock_if_reader_returns_true_first_time)
{
    Phantom::async_reader_writer_lock reader_writer_lock;
    Phantom::Coroutines::inline_scheduler scheduler;

    bool readerCalledWithRead = false;
    bool readerCalledWithWrite = false;

    co_await execute_conditional_read_unlikely_write_operation(
        reader_writer_lock,
        scheduler,
        [&](bool hasWriteLock) -> cppcoro::task<bool>
    {
        if (hasWriteLock)
        {
            EXPECT_EQ(true, readerCalledWithRead);
            EXPECT_EQ(false, readerCalledWithWrite);
            readerCalledWithWrite = true;
            co_return true;
        }
        else
        {
            EXPECT_EQ(false, readerCalledWithRead);
            EXPECT_EQ(false, readerCalledWithWrite);
            readerCalledWithRead = true;
            co_return false;
        }
    },
        [&]() -> cppcoro::task<>
    {
        EXPECT_TRUE(false);
        co_return;
    }
    );

    EXPECT_EQ(true, readerCalledWithRead);
    EXPECT_EQ(true, readerCalledWithWrite);
}

ASYNC_TEST(execute_conditional_read_unlikely_write_operation_tests, two_argument_version_does_call_writer_with_write_lock_if_reader_returns_false_second_time)
{
    Phantom::async_reader_writer_lock reader_writer_lock;
    Phantom::Coroutines::inline_scheduler scheduler;

    bool readerCalledWithRead = false;
    bool readerCalledWithWrite = false;
    bool writerCalledWithWrite = false;

    co_await execute_conditional_read_unlikely_write_operation(
        reader_writer_lock,
        scheduler,
        [&](bool hasWriteLock) -> cppcoro::task<bool>
    {
        if (hasWriteLock)
        {
            EXPECT_EQ(true, readerCalledWithRead);
            EXPECT_EQ(false, readerCalledWithWrite);
            readerCalledWithWrite = true;
        }
        else
        {
            EXPECT_EQ(false, readerCalledWithRead);
            EXPECT_EQ(false, readerCalledWithWrite);
            readerCalledWithRead = true;
        }
        co_return false;
    },
        [&]() -> cppcoro::task<>
    {
        EXPECT_FALSE(writerCalledWithWrite);
        writerCalledWithWrite = true;
        co_return;
    }
    );

    EXPECT_EQ(true, readerCalledWithRead);
    EXPECT_EQ(true, readerCalledWithWrite);
    EXPECT_EQ(true, writerCalledWithWrite);
}

}