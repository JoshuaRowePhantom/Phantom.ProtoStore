#include "Phantom.ProtoStore/src/MessageStoreImpl.h"
#include "Phantom.ProtoStore/src/MemoryExtentStore.h"
#include <gtest/gtest.h>
#include "ProtoStoreTest.pb.h"
#include <google/protobuf/util/message_differencer.h>

using std::make_shared;
using google::protobuf::util::MessageDifferencer;

namespace Phantom::ProtoStore
{
    TEST(RandomReaderWriterTest, Can_read_what_was_written)
    {
        run_async([]() -> task<> 
        {
            MessageStoreTestMessage expectedMessage;
            expectedMessage.set_string_value("hello world!");

            auto extentStore = make_shared<MemoryExtentStore>();
            auto messageStore = make_shared<MessageStore>(
                extentStore);
            auto randomMessageWriter = co_await messageStore->OpenExtentForRandomWriteAccess(0);

            co_await randomMessageWriter->Write(
                0,
                expectedMessage);

            auto randomMessageReader = co_await messageStore->OpenExtentForRandomReadAccess(0);

            MessageStoreTestMessage actualMessage;

            co_await randomMessageReader->Read(
                0,
                actualMessage);

            ASSERT_TRUE(MessageDifferencer::Equals(
                expectedMessage,
                actualMessage));
        });
    }
}