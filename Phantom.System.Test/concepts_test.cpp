#include "Phantom.System/concepts.h"

namespace Phantom
{

static_assert(is_span<std::span<int>>);
static_assert(is_span<std::span<int, 1>>);
static_assert(!is_span<int>);

}