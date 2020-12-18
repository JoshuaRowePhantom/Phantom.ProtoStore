#pragma once

#include <optional>
#include <vector>
#include <cppcoro/async_generator.hpp>
#include <cppcoro/inline_scheduler.hpp>
#include <cppcoro/multi_producer_sequencer.hpp>
#include <cppcoro/sequence_barrier.hpp>

namespace Phantom
{

template<
    typename TValue,
    typename TSequenceNumber = std::uintmax_t
> class async_ring_buffer
{
    std::vector<TValue> m_values;
    cppcoro::sequence_barrier<TSequenceNumber> m_consumerSequenceBarrier;
    cppcoro::multi_producer_sequencer<TSequenceNumber> m_producerSequencer;
    cppcoro::inline_scheduler m_scheduler;
    std::atomic<TSequenceNumber> m_lastSequenceNumber;

public:
    async_ring_buffer(
        size_t bufferSize = 1
    ) : 
        m_values(
            bufferSize),
        m_consumerSequenceBarrier(
            0),
        m_producerSequencer(
            m_consumerSequenceBarrier,
            bufferSize,
            0),
        m_lastSequenceNumber(
            static_cast<TSequenceNumber>(0))
    {
        m_producerSequencer.publish(
            0);
    }

    cppcoro::task<> complete()
    {
        auto sequenceNumber = co_await m_producerSequencer.claim_one(
            m_scheduler);
        m_lastSequenceNumber.store(
            sequenceNumber,
            std::memory_order_release);
        m_producerSequencer.publish(
            sequenceNumber);
    }

    template<
        typename T
    > cppcoro::task<> push(
        T&& value)
    {
        auto sequenceNumber = co_await m_producerSequencer.claim_one(
            m_scheduler);
        m_values[sequenceNumber % m_values.size()] = std::forward<T>(
            value);
        m_producerSequencer.publish(
            sequenceNumber);
    }

    cppcoro::async_generator<TValue> enumerate()
    {
        auto sequenceNumber = 0;

        while (true)
        {
            auto highestPublishedSequenceNumber = co_await m_producerSequencer.wait_until_published(
                sequenceNumber + 1,
                sequenceNumber,
                m_scheduler);

            auto lastSequenceNumber = m_lastSequenceNumber.load(
                std::memory_order_acquire);

            do
            {
                ++sequenceNumber;

                if (sequenceNumber == lastSequenceNumber)
                {
                    co_return;
                }

                co_yield std::move(
                    m_values[sequenceNumber % m_values.size()]);

                m_consumerSequenceBarrier.publish(
                    sequenceNumber);

            } while (sequenceNumber < highestPublishedSequenceNumber);
        }
    }
};
}