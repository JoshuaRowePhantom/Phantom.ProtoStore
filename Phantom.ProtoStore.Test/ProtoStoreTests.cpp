#include "gtest/gtest.h"
#include "Phantom.ProtoStore/include/Phantom.ProtoStore.h"
#include "Phantom.ProtoStore/src/MemoryExtentStore.h"

namespace Phantom::ProtoStore
{

TEST(ProtoStoreTests, CanCreate_memory_backed_store)
{
    run_async([]() -> task<>
    {
        auto storeFactory = MakeProtoStoreFactory();
        CreateProtoStoreRequest createRequest;

        auto memoryStore = make_shared<MemoryExtentStore>();
        createRequest.ExtentStore = [=]() -> task<shared_ptr<IExtentStore>> 
        { 
            co_return memoryStore; 
        };

        auto store = co_await storeFactory->Create(
            createRequest);
    });
}

}
