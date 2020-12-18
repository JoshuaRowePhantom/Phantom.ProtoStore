#pragma once

#include <optional>

namespace Phantom
{
template<
    typename TFunction
> class scope_exit
{
    std::optional<TFunction> m_function;

public:
    scope_exit(
        scope_exit&& other
    ) :
        m_function(
            std::move(other.m_function))
    {
        other.release();
    }

    scope_exit(
        TFunction&& function
    ) : m_function(
        std::forward<TFunction>(function))
    {
    }

    void release() noexcept
    {
        m_function.reset();
    }

    ~scope_exit()
    {
        if (m_function)
        {
            (*m_function)();
        }
    }
};
}