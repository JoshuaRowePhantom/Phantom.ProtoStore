[[deprecated]]
void x_y_z() {}
void zyx()
{
    x_y_z();
}
#pragma once

#include <cppcoro/sync_wait.hpp>
#include <gtest/gtest.h>
#include "Phantom.ProtoStore/Phantom.ProtoStore.h"
#include "Phantom.ProtoStore/src/StandardTypes.h"
#include "Phantom.System/async_utility.h"
#include <google/protobuf/util/message_differencer.h>
#include <random>
#include <filesystem>
#include "Phantom.ProtoStore/src/ExtentName.h"
#include "async_test.h"

namespace Phantom::ProtoStore
{

using google::protobuf::util::MessageDifferencer;

std::function<task<shared_ptr<IExtentStore>>()> UseMemoryExtentStore();

std::string MakeRandomString(
    std::ranlux48& rng,
    size_t length);

std::vector<std::string> MakeRandomStrings(
    std::ranlux48& rng,
    size_t stringLength,
    size_t stringCount);

std::filesystem::path MakeCleanTestDirectory(
    string testName);

shared_ptr<IExtentStore> MakeFilesystemStore(
    string testName,
    string prefix,
    size_t blockSize = 4096);

shared_ptr<IExtentStore> MakeTestExtentStore(
    string testName,
    string prefix,
    size_t blockSize = 4096);

std::function<task<shared_ptr<IExtentStore>>()> UseFilesystemStore(
    string testName,
    string prefix,
    size_t blockSize = 4096);

std::function<task<shared_ptr<IExtentStore>>()> UseTestExtentStore(
    string testName,
    string prefix,
    size_t blockSize = 4096);

}


#ifdef NDEBUG
#define PerformanceTest(name) name
#else
#define PerformanceTest(name) DISABLED_ ## name
#endif
#define DebugPerformanceTest(name) name
