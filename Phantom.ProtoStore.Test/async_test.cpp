#include "async_test.h"
#include "Phantom.Coroutines/static_thread_pool.h"
#include "Phantom.Coroutines/sync_wait.h"

namespace Phantom::ProtoStore::Test
{

void ExecuteTest(
    ::Phantom::Coroutines::reusable_task<> setupTask,
    ::Phantom::Coroutines::reusable_task<> testTask,
    ::Phantom::Coroutines::reusable_task<> tearDownTask
)
{
    // Create a thread pool to ensure that if the test itself does any threading, we
    // return control back to this thread pool.
    ::Phantom::Coroutines::static_thread_pool threadPool(1);

    auto runTestBody = [&]() -> Phantom::Coroutines::reusable_task<>
    {
        std::exception_ptr exception;
        try
        {
            co_await setupTask.when_ready();
            co_await threadPool.schedule();
            co_await setupTask;

            co_await testTask.when_ready();
            co_await threadPool.schedule();
            co_await testTask;
        }
        catch (...)
        {
            exception = std::current_exception();
        }

        try
        {
            co_await tearDownTask.when_ready();
            co_await threadPool.schedule();
            co_await tearDownTask;
        }
        catch (...)
        {
            if (!exception)
            {
                exception = std::current_exception();
            }
        }

        if (exception)
        {
            std::rethrow_exception(exception);
        }
    };

    ::Phantom::Coroutines::sync_wait(runTestBody());
}

}