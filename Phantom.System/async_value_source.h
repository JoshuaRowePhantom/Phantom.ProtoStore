#pragma once

#include "utility.h"
#include <variant>
#include <cppcoro/async_manual_reset_event.hpp>
#include <cppcoro/shared_task.hpp>
#include <cppcoro/task.hpp>

namespace Phantom
{

template<
    typename T
>
class async_value_source_operation;

template<
    typename T
>
class async_value_source
{
    std::variant<std::exception_ptr, T> m_value;
    cppcoro::async_manual_reset_event m_complete;
    cppcoro::shared_task<T&> m_task;

public:
    async_value_source()
    {}

    template <
        typename ... TArgs
    > T& emplace(
        TArgs&&... args
    )
    {
        m_value.emplace<1>(
            std::forward<TArgs>(args)...);

        m_complete.set();
        return get<1>(m_value);
    }

    bool is_set() const
    {
        return m_complete.is_set();
    }

    void unhandled_exception() noexcept
    {
        m_value.emplace<0>(
            std::current_exception());

        m_complete.set();
    }

    [[nodiscard]]
    cppcoro::task<T&> wait()
    {
        co_await m_complete;
        if (m_value.index() == 0)
        {
            std::rethrow_exception(
                get<0>(m_value));
        }
        co_return get<1>(m_value);
    }
};

}