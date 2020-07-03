#include "StandardIncludes.h"

#include "Phantom.ProtoStore/src/MemoryExtentStore.h"

namespace Phantom::ProtoStore
{

TEST(ProtoStoreTests, CanCreate_memory_backed_store)
{
    run_async([]() -> task<>
    {
        auto storeFactory = MakeProtoStoreFactory();
        CreateProtoStoreRequest createRequest;

        createRequest.ExtentStore = UseMemoryExtentStore();

        auto store = co_await storeFactory->Create(
            createRequest);
    });
}

}
