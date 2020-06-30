#include <memory>

namespace Phantom::System
{
    template<typename T>
    struct pooled_deleter
    {
        void operator()(T* p) const
        {
            ReturnToPool(p);
        }
    };

    template<typename T>
    using pooled_ptr = std::unique_ptr<T, pooled_deleter<T>>;

    template<typename T, typename ... TArgs>
    pooled_ptr<T> make_pooled(
        TArgs&&... args)
    {
        return pooled_ptr(
            new T(std::forward<TArgs>(args)...));
    }

    template<typename TPooled>
    concept Pooled = requires (TPooled p)
    {
        p.ReturnToPool();
    };

    template<Pooled P>
    void ReturnToPool(
        P* p
    )
    {
        p->ReturnToPool();
    }
}