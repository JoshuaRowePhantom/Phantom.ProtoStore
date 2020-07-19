#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/MemoryExtentStore.h"

namespace Phantom::ProtoStore
{

std::function<task<shared_ptr<IExtentStore>>()> UseMemoryExtentStore()
{
    auto extentStore = make_shared<MemoryExtentStore>();
    auto lambda = [=]() -> task<shared_ptr<IExtentStore>>
    {
        co_return extentStore;
    };

    return lambda;
}

std::string MakeRandomString(
    std::mt19937& rng,
    size_t length)
{
    std::uniform_int_distribution<int> distribution('a', 'z');
    string randomString(' ', length);
    for (int stringIndex = 0; stringIndex < randomString.size(); stringIndex++)
    {
        randomString[stringIndex] = distribution(rng);
    }
    return randomString;
}

std::vector<std::string> MakeRandomStrings(
    std::mt19937& rng,
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

}
