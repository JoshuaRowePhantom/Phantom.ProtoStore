#pragma once

#include "utility.h"
#include <atomic>
#include <exception>
#include <optional>

namespace Phantom
{

namespace detail
{

extern empty has_exception;
extern empty has_value;

}

template<
    typename T
>
class async_value_source_operation;

template<
    typename T
>
class async_value_source
{
    template<
        typename T
    > friend class async_value_source_operation;

    // nullptr = not set, nobody waiting.
    // &has_exception = nobody waiting, has an exception
    // &has_value = nobody waiting, has a value
    // Otherwise it's a linked list of waiters.
    mutable std::atomic<void*> m_state;
    
    union 
    {
        mutable std::remove_const_t<T> m_value;
        mutable std::exception_ptr m_exception;
    };

    void signal_waiters(
        void* newState)
    {
        void* oldState = m_state.exchange(
            newState,
            std::memory_order_acq_rel);

        assert(oldState != &detail::has_exception);
        assert(oldState != &detail::has_value);

        auto current = static_cast<async_value_source_operation<T>*>(oldState);
        while (current)
        {
            auto next = current->m_next;
            current->m_awaiter.resume();
            current = next;
        }
    }

public:
    async_value_source()
        : m_state(nullptr)
    {}

    ~async_value_source()
    {
        auto state = m_state.load(
            std::memory_order_relaxed);

        if (state == &detail::has_value)
        {
            m_value.~T();
        }
        else if (state == &detail::has_exception)
        {
            m_exception.~exception_ptr();
        } 
        else
        {
            assert(!state);
        }
    }

    template <
        typename ... TArgs
    > T& emplace(
        TArgs&&... args
    )
    {
        auto& result = *(new (&m_value) T(
            std::forward<TArgs>(args)...));

        signal_waiters(
            &detail::has_value);

        return result;
    }

    bool is_set() const
    {
        auto state = m_state.load(
            std::memory_order_relaxed);

        return state == &detail::has_value
            || state == &detail::has_exception;
    }

    void unhandled_exception() noexcept
    {
        new (&m_exception) std::exception_ptr(
            std::current_exception());

        signal_waiters(
            &detail::has_exception);
    }

    [[nodiscard]]
    async_value_source_operation<T> operator co_await() const noexcept
    {
        return async_value_source_operation<T>(
            *this);
    }
};

template<
    typename T
>
class async_value_source_operation
{
    template<
        typename T
    >
    friend class async_value_source;
    const async_value_source<T>& m_valueSource;
    async_value_source_operation* m_next;
    std::experimental::coroutine_handle<> m_awaiter;

public:
    explicit async_value_source_operation(
        const async_value_source<T>& valueSource)
        :
        m_valueSource(valueSource)
    {}

    bool await_ready() const noexcept
    {
        auto state = m_valueSource.m_state.load(
            std::memory_order_relaxed);
        
        return state == &detail::has_value
            || state == &detail::has_exception;
    }

    bool await_suspend(
        std::experimental::coroutine_handle<> awaiter
    ) noexcept
    {
        m_awaiter = awaiter;

        void* oldState = m_valueSource.m_state.load(
            std::memory_order_relaxed);
        
        do
        {
            if (oldState == &detail::has_value
                || oldState == &detail::has_exception)
            {
                return false;
            }

            m_next = static_cast<async_value_source_operation*>(
                oldState);

        } while (!m_valueSource.m_state.compare_exchange_weak(
            oldState,
            static_cast<void*>(this),
            std::memory_order_release,
            std::memory_order_acquire));

        return true;
    }

    T& await_resume()
    {
        auto state = m_valueSource.m_state.load(
            std::memory_order_relaxed);

        if (state == &detail::has_value)
        {
            return m_valueSource.m_value;
        }
        else
        {
            std::rethrow_exception(
                m_valueSource.m_exception);
        }
    }
};

}