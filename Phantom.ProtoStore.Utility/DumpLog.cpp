#include "Utility.h"
#include <flatbuffers/flatbuffers.h>
#include <flatbuffers/minireflect.h>

using namespace std;
using namespace Phantom::ProtoStore;


task<> DumpLog(
    string logPath)
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

    auto logExtent = co_await extentStore->OpenExtentForRead(
        logPath);

    auto logMessageReader = co_await messageStore->OpenExtentForSequentialReadAccess(
        logExtent);

    do
    {
        auto readMessageResult = co_await logMessageReader->Read();
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
            "LogRecord",
            flatbuffers::FlatBufferToString(
                span.data(),
                FlatBuffers::LogRecordTypeTable(),
                true,
                true,
                "  "),
            readMessageResult->DataRange.Beginning);
    } while (true);

}