#include "ExtentName.h"
#include "ProtoStoreFactory.h"
#include "ProtoStore.h"

namespace Phantom::ProtoStore
{
task<shared_ptr<IProtoStore>> ProtoStoreFactory::Open(
    const OpenProtoStoreRequest& openRequest)
{
    auto extentStore = co_await openRequest.ExtentStore();

    auto protoStore = make_shared<ProtoStore>(
        openRequest.Schedulers,
        extentStore);

    co_await protoStore->Open(
        openRequest);

    co_return protoStore;
}

task<shared_ptr<IProtoStore>> ProtoStoreFactory::Create(
    const CreateProtoStoreRequest& createRequest)
{
    auto extentStore = co_await createRequest.ExtentStore();

    auto protoStore = make_shared<ProtoStore>(
        createRequest.Schedulers,
        extentStore);

    co_await protoStore->Create(
        createRequest);

    co_return protoStore;
}

shared_ptr<IProtoStoreFactory> MakeProtoStoreFactory()
{
    return make_shared<ProtoStoreFactory>();
}
}
