#pragma once
#include <atomic>
#include <utility>

namespace Phantom::ProtoStore
{

template<
    typename T
>
class AtomicSequenceNumber
{
    std::atomic<T> m_nextSequenceNumber;

public:
    AtomicSequenceNumber(
        T value
    ) :
        m_nextSequenceNumber(value)
    {}

    AtomicSequenceNumber(
        const AtomicSequenceNumber& other
    ) :
        m_nextSequenceNumber(other.m_nextSequenceNumber.load(std::memory_order_relaxed))
    {}

    AtomicSequenceNumber& operator=(const AtomicSequenceNumber& other)
    {
        m_nextSequenceNumber = other.m_nextSequenceNumber.load(std::memory_order_relaxed);
        return *this;
    }

    T AllocateNextSequenceNumber()
    {
        return m_nextSequenceNumber.fetch_add(1, std::memory_order_relaxed);
    }

    T GetNextSequenceNumber() const
    {
        return m_nextSequenceNumber.load(std::memory_order_relaxed);
    }
    
    T GetCurrentSequenceNumber() const
    {
        return m_nextSequenceNumber.load(std::memory_order_relaxed) - 1;
    }

    void ReplayUsedSequenceNumber(
        T value
    )
    {
        ReplayNextSequenceNumber(
            value + 1);
    }

    void ReplayNextSequenceNumber(
        T value
    )
    {
        T expected = m_nextSequenceNumber.load(std::memory_order_relaxed);
        T next;
        do
        {
            next = std::max(expected, value);
        } while (!m_nextSequenceNumber.compare_exchange_weak(
            expected,
            next,
            std::memory_order_relaxed));
    }
};

}
