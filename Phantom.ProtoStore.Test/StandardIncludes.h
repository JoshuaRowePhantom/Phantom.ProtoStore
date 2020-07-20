#pragma once

#include <cppcoro/sync_wait.hpp>
#include <gtest/gtest.h>
#include "Phantom.ProtoStore/Phantom.ProtoStore.h"
#include "Phantom.ProtoStore/src/StandardTypes.h"
#include <google/protobuf/util/message_differencer.h>
#include <random>
#include <filesystem>

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

std::filesystem::path MakeCleanTestDirectory(
    string testName);

shared_ptr<IExtentStore> MakeFilesystemStore(
    string testName,
    string prefix,
    size_t blockSize);

std::function<task<shared_ptr<IExtentStore>>()> UseFilesystemStore(
    string testName,
    string prefix,
    size_t blockSize);

}


#ifdef NDEBUG
#define PerformanceTest(name) name
#else
#define PerformanceTest(name) DISABLED_ ## name
#endif
