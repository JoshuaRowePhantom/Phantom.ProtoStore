#pragma once

#include <atomic>
#include <cppcoro/inline_scheduler.hpp>
#include <cppcoro/sequence_barrier.hpp>
#include "utility.h"

namespace Phantom
{
// This class ensures that each invocation of spawn()
// immediately starts a new task, but ensure that the resulting
// task completes only after all previous invocations of
// spawn() have completed.
// The intent is that each call to spawn starts some process
// that should only be reported as long as it and all previous
// calls have completed, but can otherwise proceed independently.
template<
    typename TSequenceNumber = std::uint_fast64_t
>
class encompassing_pending_task
{
private:
    cppcoro::sequence_barrier<TSequenceNumber> m_sequenceBarrier;
    std::atomic<TSequenceNumber> m_nextSequenceNumber;
    cppcoro::inline_scheduler m_scheduler;

    template<
        typename TAwaitable
    > cppcoro::task<> run(
        TAwaitable awaitable,
        TSequenceNumber sequenceNumberToWaitFor,
        TSequenceNumber sequenceNumberOfThisOperation
    )
    {
        auto task = cppcoro::make_task(
            std::forward<TAwaitable>(
                awaitable));

        co_await task.when_ready();
        
        co_await m_sequenceBarrier.wait_until_published(
            sequenceNumberToWaitFor,
            m_scheduler
            );

        m_sequenceBarrier.publish(
            sequenceNumberOfThisOperation);

        co_await task;
    }

public:

    encompassing_pending_task()
        :
        m_nextSequenceNumber(0),
        m_sequenceBarrier(0)
    {
    }

    template<
        typename TAwaitable
    >
    cppcoro::task<> spawn(
        TAwaitable awaitable)
    {
        auto sequenceNumberToWaitFor = m_nextSequenceNumber.fetch_add(
            1,
            std::memory_order_acq_rel
        );

        auto sequenceNumberOfThisOperation = sequenceNumberToWaitFor + 1;

        return run(
            std::forward<TAwaitable>(awaitable),
            sequenceNumberToWaitFor,
            sequenceNumberOfThisOperation
        );
    }

    cppcoro::task<> join()
    {
        co_await m_sequenceBarrier.wait_until_published(
            m_nextSequenceNumber.load(std::memory_order_acquire),
            m_scheduler);
    }
};

}