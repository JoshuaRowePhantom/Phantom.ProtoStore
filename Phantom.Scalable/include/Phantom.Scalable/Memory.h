#pragma once

#include <memory>

namespace Phantom::Scalable
{

template<
    typename T
> class downcasting_shared_cast
{
public:
    template<
        typename TTarget
    > std::shared_ptr<TTarget> shared_cast()
    {
        return std::shared_ptr<TTarget>(
            static_cast<T*>(this)->shared_from_this());
    }

    template<
        typename TTarget
    > std::shared_ptr<const TTarget> shared_cast() const
    {
        return std::shared_ptr<TTarget>(
            static_cast<const T*>(this)->shared_from_this());
    }
};

template<
    typename T
>
class downcasting_enable_shared_from_this
    :
    public std::enable_shared_from_this<T>,
    public downcasting_shared_cast<T>
{
};

}
