#include <type_traits>

namespace Phantom
{
// Conditionally move an rvalue reference back to another rvalue reference.
template<
    typename T1,
    typename T2
> void put_back(
    const T1& destination,
    const T2& source
)
{
    // In this case, do nothing, because the destination is not
    // an rvalue reference.
}

// Conditionally move an rvalue reference back to another rvalue reference.
template<
    typename T1,
    typename T2
> void put_back(
    T1& destination,
    T2&& source
)
{
    // In this case, do nothing, because the destination is not
    // an rvalue reference.
}

// Conditionally move an rvalue reference back to another rvalue reference.
template<
    typename T1,
    typename T2,
    typename enabled = typename std::enable_if<std::is_assignable_v<T1, T2>>::type
> T1& put_back(
    T1&& destination,
    T2&& source
)
{
    return destination = std::forward<T2>(source);
}

}
