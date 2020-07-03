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

TEST(ProtoStoreTests, CanOpen_memory_backed_store)
{
    run_async([]() -> task<>
    {
        auto storeFactory = MakeProtoStoreFactory();
        CreateProtoStoreRequest createRequest;

        createRequest.ExtentStore = UseMemoryExtentStore();

        auto store = co_await storeFactory->Create(
            createRequest);
        store = co_await storeFactory->Open(
            createRequest);
    });
}

TEST(ProtoStoreTests, Open_fails_on_uncreated_store)
{
    run_async([]() -> task<>
    {
        auto storeFactory = MakeProtoStoreFactory();
        OpenProtoStoreRequest openRequest;

        openRequest.ExtentStore = UseMemoryExtentStore();

        ASSERT_THROW(
            co_await storeFactory->Open(
                openRequest),
            range_error);
    });
}

}
