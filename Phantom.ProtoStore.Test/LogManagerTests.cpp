#include "StandardIncludes.h"

#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"
#include "Phantom.ProtoStore/src/LogManager.h"
#include "Phantom.ProtoStore/src/MessageStore.h"
#include "TestFactories.h"

namespace Phantom::ProtoStore
{

class LogManagerTests 
    :
    public testing::Test,
    public TestFactories
{
protected:
    using DatabaseHeaderT = FlatBuffers::DatabaseHeaderT;

    struct LogManagerTest
    {
        shared_ptr<LogManager> logManager;
        shared_ptr<IExtentStore> logExtentStore;
        shared_ptr<IMessageStore> logMessageStore;
    };

    task<LogManagerTest> CreateTest(
        std::string testName
    )
    {
        auto schedulers = Schedulers::Inline();
        auto logExtentStore = MakeTestExtentStore("LogManagerTests", testName);
        auto logMessageStore = MakeMessageStore(
            schedulers,
            logExtentStore);

        auto logManager = std::make_shared<LogManager>(
            schedulers,
            logExtentStore,
            logMessageStore
        );

        co_return LogManagerTest
        {
            .logManager = logManager,
            .logExtentStore = logExtentStore,
            .logMessageStore = logMessageStore,
        };
    }
};

ASYNC_TEST_F(LogManagerTests, Can_create_and_destroy)
{
    auto logManagerTest = co_await CreateTest(
        "Can_create_and_destroy");
    co_return;
}

}

