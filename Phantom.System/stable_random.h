#pragma once

#include <random>

namespace Phantom
{
// For now, just delegate to the platform.  We'll fix it up later.
using stable_int_distribution = std::uniform_int_distribution;
}