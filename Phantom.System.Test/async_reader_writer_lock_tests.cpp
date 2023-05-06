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
}