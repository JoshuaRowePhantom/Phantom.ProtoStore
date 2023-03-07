#include "Utility.h"
#include <flatbuffers/flatbuffers.h>
#include <flatbuffers/minireflect.h>

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

    FlatMessage<FlatBuffers::PartitionMessage> message;
    do
    {
        auto readMessageResult = co_await dataMessageReader->Read();
        auto span = get_uint8_t_span(readMessageResult->Content.Payload);

        if (!span.data())
        {
            co_return;
        }

        flatbuffers::Verifier verifier(
            span.data(),
            span.size());
        if (!verifier.VerifyBuffer<FlatBuffers::LogRecord>())
        {
            std::cout << "Invalid message!\n";
        }

        DumpMessage(
            "PartitionMessage",
            flatbuffers::FlatBufferToString(
                span.data(),
                FlatBuffers::PartitionMessageTypeTable(),
                true,
                true,
                "  "),
            readMessageResult->DataRange.Beginning);
    } while (true);

}