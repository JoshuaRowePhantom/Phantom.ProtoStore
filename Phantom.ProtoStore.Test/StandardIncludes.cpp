#include "StandardIncludes.h"
#include "Phantom.ProtoStore/numeric_cast.h"
#include "Phantom.ProtoStore/src/MemoryExtentStore.h"
#include "Phantom.ProtoStore/src/MemoryMappedFileExtentStore.h"

#if PHANTOM_USE_MIMALLOC
#include "mimalloc-new-delete.h"
#endif

namespace Phantom::ProtoStore
{

#if PHANTOM_USE_MIMALLOC
auto mimalloc_version = mi_version();
#endif

std::function<task<shared_ptr<IExtentStore>>()> UseMemoryExtentStore()
{
    auto extentStore = make_shared<MemoryExtentStore>(
        Schedulers::Default());
    auto lambda = [=]() -> task<shared_ptr<IExtentStore>>
    {
        co_return extentStore;
    };

    return lambda;
}

std::string MakeRandomString(
    std::ranlux48& rng,
    size_t length)
{
    std::uniform_int_distribution<int> distribution('a', 'z');
    string randomString(length, ' ');
    for (int stringIndex = 0; stringIndex < randomString.size(); stringIndex++)
    {
        randomString[stringIndex] = numeric_cast(distribution(rng));
    }
    return randomString;
}

std::vector<std::string> MakeRandomStrings(
    std::ranlux48& rng,
    size_t stringLength,
    size_t stringCount)
{
    std::vector<std::string> strings;
    strings.reserve(stringCount);

    for (int stringCounter = 0; stringCounter < stringCount; stringCounter++)
    {
        strings.push_back(
            MakeRandomString(
                rng,
                stringLength));
    }

    return strings;
}

std::filesystem::path MakeCleanTestDirectory(
    string testName)
{
    auto path = std::filesystem::temp_directory_path() / "Phantom.ProtoStore.Tests" / testName;
    std::filesystem::remove_all(
        path);
    std::filesystem::create_directories(
        path);

    return path;
}

shared_ptr<IExtentStore> MakeFilesystemStore(
    string testName,
    string storeName,
    size_t blockSize)
{
    auto path = MakeCleanTestDirectory(
        testName + "_" + storeName
    ) / "";

    auto store = make_shared<MemoryMappedFileExtentStore>(
        Schedulers::Default(),
        path.string(),
        ".dat",
        blockSize,
        MemoryMappedFileExtentStore::ExtentDeleteAction::Rename
        );

    return store;
}

std::function<task<shared_ptr<IExtentStore>>()> UseFilesystemStore(
    string testName,
    string storeName,
    size_t blockSize)
{
    auto store = MakeFilesystemStore(
        testName,
        storeName,
        blockSize);

    return [store]() -> task<shared_ptr<IExtentStore>>
    {
        co_return store;
    };
}

shared_ptr<IExtentStore> MakeTestExtentStore(
    string testName,
    string prefix,
    size_t blockSize)
{
    return MakeFilesystemStore(
        testName,
        prefix,
        blockSize);
}

std::function<task<shared_ptr<IExtentStore>>()> UseTestExtentStore(
    string testName,
    string storeName,
    size_t blockSize)
{
    return UseFilesystemStore(
        testName,
        storeName,
        blockSize);
}

}
