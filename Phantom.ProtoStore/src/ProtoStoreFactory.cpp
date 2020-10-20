#include "ExtentName.h"
#include "ProtoStoreFactory.h"
#include "ProtoStore.h"

namespace Phantom::ProtoStore
{
task<shared_ptr<IProtoStore>> ProtoStoreFactory::Open(
    const OpenProtoStoreRequest& openRequest)
{
    auto headerExtentStore = co_await openRequest.HeaderExtentStore();
    auto logExtentStore = co_await openRequest.LogExtentStore();
    auto dataExtentStore = co_await openRequest.DataExtentStore();
    auto dataHeaderExtentStore = co_await openRequest.DataHeaderExtentStore();

    auto protoStore = make_shared<ProtoStore>(
        openRequest.Schedulers,
        headerExtentStore,
        logExtentStore,
        dataExtentStore,
        dataHeaderExtentStore);

    co_await protoStore->Open(
        openRequest);

    co_return protoStore;
}

task<shared_ptr<IProtoStore>> ProtoStoreFactory::Create(
    const CreateProtoStoreRequest& createRequest)
{
    auto headerExtentStore = co_await createRequest.HeaderExtentStore();
    auto logExtentStore = co_await createRequest.LogExtentStore();
    auto dataExtentStore = co_await createRequest.DataExtentStore();
    auto dataHeaderExtentStore = co_await createRequest.DataHeaderExtentStore();

    auto protoStore = make_shared<ProtoStore>(
        createRequest.Schedulers,
        headerExtentStore,
        logExtentStore,
        dataExtentStore,
        dataHeaderExtentStore);

    co_await protoStore->Create(
        createRequest);

    co_return protoStore;
}

shared_ptr<IProtoStoreFactory> MakeProtoStoreFactory()
{
    return make_shared<ProtoStoreFactory>();
}
}
