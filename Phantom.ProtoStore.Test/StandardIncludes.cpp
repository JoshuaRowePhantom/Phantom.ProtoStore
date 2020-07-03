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

}