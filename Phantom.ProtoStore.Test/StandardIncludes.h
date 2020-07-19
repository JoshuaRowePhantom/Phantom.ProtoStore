#pragma once

#include <cppcoro/sync_wait.hpp>
#include <gtest/gtest.h>
#include "Phantom.ProtoStore/Phantom.ProtoStore.h"
#include "Phantom.ProtoStore/src/StandardTypes.h"
#include <google/protobuf/util/message_differencer.h>
#include <random>

namespace Phantom::ProtoStore
{

using google::protobuf::util::MessageDifferencer;

std::function<task<shared_ptr<IExtentStore>>()> UseMemoryExtentStore();

std::string MakeRandomString(
    std::mt19937& rng,
    size_t length);

std::vector<std::string> MakeRandomStrings(
    std::mt19937& rng,
    size_t stringLength,
    size_t stringCount);

}


#ifdef NDEBUG
#define PerformanceTest(name) name
#else
#define PerformanceTest(name) DISABLED_ ## name
#endif
