#include "Utility.h"

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

    LogRecord message;
    try
    {
        do
        {
            auto readMessageResult = co_await logMessageReader->Read(
                message);

            DumpMessage(
                "LogRecord",
                message,
                readMessageResult.DataRange.Beginning
            );
        } while (true);
    }
    catch (range_error)
    { }

}