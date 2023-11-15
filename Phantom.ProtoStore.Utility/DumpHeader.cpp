#include "Utility.h"
#include <flatbuffers/flatbuffers.h>
#include <flatbuffers/minireflect.h>

using namespace std;
using namespace Phantom::ProtoStore;

task<> DumpHeader(
    string headerPath)
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

    auto headerExtent = co_await extentStore->OpenExtentForRead(
        headerPath);

    auto dataMessageReader = co_await messageStore->OpenExtentForSequentialReadAccess(
        headerExtent);

    FlatMessage<FlatBuffers::DatabaseHeader> message;
    auto readMessageResult = co_await dataMessageReader->Read();
    auto span = get_uint8_t_span(readMessageResult->Content.Payload);

    if (!span.data())
    {
        co_return;
    }

    flatbuffers::Verifier verifier(
        span.data(),
        span.size());
    if (!verifier.VerifyBuffer<FlatBuffers::DatabaseHeader>())
    {
        std::cout << "Invalid message!\n";
    }

    DumpMessage(
        "DatabaseHeader",
        flatbuffers::FlatBufferToString(
            span.data(),
            FlatBuffers::DatabaseHeaderTypeTable(),
            true,
            true,
            "  "),
        readMessageResult->DataRange.Beginning);

}