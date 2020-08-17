#pragma once

#include <gtest/gtest.h>
#include "Phantom.System/async_utility.h"
#include "Phantom.System/utility.h"

#ifdef NDEBUG
#define PerformanceTest(name) name
#else
#define PerformanceTest(name) DISABLED_ ## name
#endif
