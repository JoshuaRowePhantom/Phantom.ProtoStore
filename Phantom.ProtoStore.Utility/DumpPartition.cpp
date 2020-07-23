#include "src/ProtoStoreInternal.pb.h"
#include "Phantom.ProtoStore/src/MemoryMappedFileExtentStore.h"
#include "Phantom.ProtoStore/src/MessageStore.h"
#include <string>
#include <iostream>
#include <sstream>

using namespace std;
using namespace Phantom::ProtoStore;

void DumpMessage(
    string name,
    Message& message,
    ExtentOffset offset
)
{
    cout << name << " @ [" << offset << "]\n" << message.DebugString() << "\n";
}

task<> DumpTreeNode(
    shared_ptr<IRandomMessageReader> dataMessageReader,
    ExtentOffset offset,
    int level
)
{
    PartitionTreeNode treeNode;
    
    co_await dataMessageReader->Read(
        offset,
        treeNode);

    ostringstream messageName;
    messageName << "PartitionTreeNode[" << level << "]";

    DumpMessage(
        messageName.str(),
        treeNode,
        offset);

    for (auto& treeNodeEntry : treeNode.treeentries())
    {
        if (treeNodeEntry.PartitionTreeEntryType_case() == PartitionTreeEntry::kTreeNodeOffset)
        {
            co_await DumpTreeNode(
                dataMessageReader,
                treeNodeEntry.treenodeoffset(),
                level + 1);
        }
    }
}

task<> DumpPartition(
    string headerPath,
    string dataPath)
{
    auto extentStore = make_shared<MemoryMappedFileExtentStore>(
        Schedulers::Default(),
        "nonexistent",
        "dummy",
        4096);

    auto messageStore = MakeMessageStore(
        Schedulers::Default(),
        extentStore);

    auto headerExtent = co_await extentStore->OpenExtentForRead(
        headerPath);

    auto headerMessageReader = co_await messageStore->OpenExtentForRandomReadAccess(
        headerExtent);

    auto dataExtent = co_await extentStore->OpenExtentForRead(
        dataPath);

    auto dataMessageReader = co_await messageStore->OpenExtentForSequentialReadAccess(
        dataExtent);

    PartitionMessage message;
    do
    {
        auto readMessageResult = co_await dataMessageReader->Read(
            message);

        DumpMessage(
            "PartitionMessage",
            message,
            readMessageResult.DataRange.Beginning
        );
    } while (!message.has_partitionheader());

}