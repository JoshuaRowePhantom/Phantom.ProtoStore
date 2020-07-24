#include <atomic>

namespace Phantom
{

template<
    typename TAtomic,
    typename TTransform,
    typename ... TArgs
>
bool compare_exchange_engine(
    std::atomic<TAtomic>& atomic,
    TTransform conditionAndTransform,
    std::memory_order loadOrder,
    bool (std::atomic<TAtomic>::* modifier)(
        TAtomic&,
        const TAtomic,
        const TArgs...),
    TArgs... memoryOrderArgs)
{
    auto expected = atomic.load(loadOrder);
    while (true)
    {
        auto desired = expected;
        if (!conditionAndTransform(desired))
        {
            return false;
        }

        if ((atomic.*modifier)(
            expected,
            desired,
            memoryOrderArgs...))
        {
            return true;
        }
    }
}

template<
    typename TAtomic,
    typename TLambda,
    typename ... TArgs
>
void compare_exchange_transform(
    std::atomic<TAtomic>& atomic,
    TLambda transform,
    std::memory_order loadOrder,
    bool (std::atomic<TAtomic>::* modifier)(
        TAtomic&,
        const TAtomic,
        const TArgs...),
    TArgs ... memoryOrderArgs
    )
{
    compare_exchange_engine(
        atomic,
        [transform](auto& value)
    {
        transform(value);
        return true;
    },
        loadOrder,
        modifier,
        memoryOrderArgs...
    );
}

template<
    typename TAtomic,
    typename TLambda,
    typename ... TMemoryOrderArgs
>
void compare_exchange_weak_transform(
    std::atomic<TAtomic>& atomic,
    TLambda transform,
    std::memory_order loadOrder,
    const TMemoryOrderArgs ... memoryOrderArgs
    )
{
    compare_exchange_transform(
        atomic,
        transform,
        loadOrder,
        &std::atomic<TAtomic>::compare_exchange_weak,
        memoryOrderArgs...
        );
}

}