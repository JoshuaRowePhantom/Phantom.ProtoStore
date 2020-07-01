#pragma once

#include <memory>
#include "utility.h"

namespace Phantom::System
{
    template<typename TPooled>
    concept SelfReturnable = requires (TPooled * p)
    {
        p->ReturnToPool();
    };

    template<SelfReturnable P>
    void ReturnToPool(
        P* p
    )
    {
        p->ReturnToPool();
    }

    template<typename TPooled>
    concept Returnable = requires (TPooled* p)
    {
        ReturnToPool(p);
    };

    template<typename T>
    struct pooled_deleter
    {
        static_assert(always_false<T>, "pooled_deleter<T> must be defined; Probably need a ReturnToPool method.");
    };

    template<Returnable T>
    struct pooled_deleter<T>
    {
        void operator()(T* p) const
        {
            ReturnToPool(p);
        }
    };

    template<
        typename T, 
        typename D = pooled_deleter<T>
    >
    using pooled_ptr = std::unique_ptr<T, D>;

    template<
        typename T, 
        typename ... TArgs
    >
    pooled_ptr<T> make_pooled(
        TArgs&&... args)
    {
        return pooled_ptr<T>(
            new T(std::forward<TArgs>(args)...));
    }
}