#include "StandardIncludes.h"
#include "Phantom.System/async_utility.h"
#include <cppcoro/async_generator.hpp>

namespace Phantom
{
static_assert(as_awaitable_iterator<cppcoro::async_generator<int>::iterator>);
static_assert(can_co_await<decltype(std::declval<cppcoro::async_generator<int>>().begin())>);
static_assert(as_awaitable_iterator<decltype(as_sync_awaitable(std::declval<cppcoro::async_generator<int>>().begin()))>);
static_assert(as_awaitable_async_enumerable<cppcoro::async_generator<int>>);
static_assert(as_awaitable_async_enumerable_of<cppcoro::async_generator<int>, int>);
}
