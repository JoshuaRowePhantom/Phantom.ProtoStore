#include "Utility.h"

using namespace std;
using namespace Phantom::ProtoStore;

task<> DumpPartition(
    string dataPath)
{
    auto extentStore = make_shared<MemoryMappedFileExtentStore>(
        Schedulers::Default(),
        "nonexistent",
        "dummy",
        4096,
        MemoryMappedFileExtentStore::ExtentDeleteAction::Rename);

    auto messageStore = MakeMessageStore(
        Schedulers::Default(),
        extentStore);

    auto dataExtent = co_await extentStore->OpenExtentForRead(
        dataPath);

    auto dataMessageReader = co_await messageStore->OpenExtentForSequentialReadAccess(
        dataExtent);

    Serialization::PartitionMessage message;
    do
    {
        auto readMessageResult = co_await dataMessageReader->Read(
            message);

        DumpMessage(
            "PartitionMessage",
            message,
            readMessageResult->DataRange.Beginning
        );
    } while (!message.has_partitionheader());

}