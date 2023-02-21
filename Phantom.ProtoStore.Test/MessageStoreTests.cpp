#include "StandardIncludes.h"

#include "Phantom.ProtoStore/src/MessageStoreImpl.h"
#include "Phantom.ProtoStore/src/MemoryExtentStore.h"
#include "ProtoStoreTest.pb.h"

namespace Phantom::ProtoStore
{
    TEST(RandomReaderWriterTest, Can_read_what_was_written)
    {
        run_async([]() -> task<>
        {
            MessageStoreTestMessage expectedMessage;
            expectedMessage.set_string_value("hello world!");

            auto extentStore = make_shared<MemoryExtentStore>(
                Schedulers::Default());
            auto messageStore = make_shared<MessageStore>(
                Schedulers::Default(),
                extentStore);
            auto randomMessageWriter = co_await messageStore->OpenExtentForRandomWriteAccess(MakeLogExtentName(0));

            co_await randomMessageWriter->Write(
                0,
                expectedMessage,
                FlushBehavior::Flush);

            auto randomMessageReader = co_await messageStore->OpenExtentForRandomReadAccess(MakeLogExtentName(0));

            MessageStoreTestMessage actualMessage;

            co_await randomMessageReader->Read(
                0,
                actualMessage);

            EXPECT_TRUE(MessageDifferencer::Equals(
                expectedMessage,
                actualMessage));
        });
    }

    TEST(RandomReaderWriterTest, Can_read_what_was_written_after_DontFlush_then_Flush)
    {
        run_async([]() -> task<>
        {
            MessageStoreTestMessage expectedMessage1;
            expectedMessage1.set_string_value("hello world 1!");
            MessageStoreTestMessage expectedMessage2;
            expectedMessage2.set_string_value("hello world 2!");

            auto extentStore = make_shared<MemoryExtentStore>(
                Schedulers::Default());
            auto messageStore = make_shared<MessageStore>(
                Schedulers::Default(),
                extentStore);
            auto randomMessageWriter = co_await messageStore->OpenExtentForRandomWriteAccess(MakeLogExtentName(0));

            auto writeResult1 = co_await randomMessageWriter->Write(
                0,
                expectedMessage1,
                FlushBehavior::DontFlush);

            auto writeResult2 = co_await randomMessageWriter->Write(
                writeResult1.DataRange.End,
                expectedMessage2,
                FlushBehavior::Flush);

            auto randomMessageReader = co_await messageStore->OpenExtentForRandomReadAccess(MakeLogExtentName(0));

            MessageStoreTestMessage actualMessage1;
            MessageStoreTestMessage actualMessage2;

            auto readResult1 = co_await randomMessageReader->Read(
                0,
                actualMessage1);

            auto readResult2 = co_await randomMessageReader->Read(
                readResult1.DataRange.End,
                actualMessage2);

            EXPECT_TRUE(MessageDifferencer::Equals(
                expectedMessage1,
                actualMessage1));

            EXPECT_TRUE(MessageDifferencer::Equals(
                expectedMessage2,
                actualMessage2));
        });
    }

    TEST(RandomReaderWriterTest, ReportedOffsets_are_at_end_of_message_plus_checksum)
    {
        run_async([]() -> task<>
        {
            MessageStoreTestMessage expectedMessage;
            expectedMessage.set_string_value("hello world!");

            auto checksumAlgorithmFactory = MakeChecksumAlgorithmFactory();
            auto checksumAlgorithm = checksumAlgorithmFactory->Create();

            size_t offset = 500;
            size_t expectedEndOfMessage =
                offset
                + expectedMessage.ByteSize()
                + sizeof(uint32_t)
                + sizeof(uint8_t)
                + checksumAlgorithm->SizeInBytes();

            auto extentStore = make_shared<MemoryExtentStore>(
                Schedulers::Default());
            auto messageStore = make_shared<MessageStore>(
                Schedulers::Default(),
                extentStore);
            auto randomMessageWriter = co_await messageStore->OpenExtentForRandomWriteAccess(MakeLogExtentName(0));

            auto writeResult = co_await randomMessageWriter->Write(
                offset,
                expectedMessage,
                FlushBehavior::Flush);

            EXPECT_EQ(expectedEndOfMessage, writeResult.DataRange.End);

            auto randomMessageReader = co_await messageStore->OpenExtentForRandomReadAccess(MakeLogExtentName(0));

            MessageStoreTestMessage actualMessage;

            auto readResult = co_await randomMessageReader->Read(
                offset,
                actualMessage);

            EXPECT_TRUE(MessageDifferencer::Equals(
                expectedMessage,
                actualMessage));

            EXPECT_EQ(expectedEndOfMessage, readResult.DataRange.End);
        });
    }

    TEST(RandomReaderWriterTest, ReadOfInvalidMessageChecksum_reports_an_error)
    {
        run_async([]() -> task<>
        {
            MessageStoreTestMessage expectedMessage;
            expectedMessage.set_string_value("hello world!");

            auto checksumAlgorithmFactory = MakeChecksumAlgorithmFactory();
            auto checksumAlgorithm = checksumAlgorithmFactory->Create();

            size_t offset = 500;
            size_t expectedEndOfMessage =
                offset
                + expectedMessage.ByteSize()
                + sizeof(uint32_t)
                + sizeof(uint8_t)
                + checksumAlgorithm->SizeInBytes();

            auto extentStore = make_shared<MemoryExtentStore>(
                Schedulers::Default());
            auto messageStore = make_shared<MessageStore>(
                Schedulers::Default(),
                extentStore);
            auto randomMessageWriter = co_await messageStore->OpenExtentForRandomWriteAccess(MakeLogExtentName(0));

            auto writeResult = co_await randomMessageWriter->Write(
                offset,
                expectedMessage,
                FlushBehavior::Flush);

            EXPECT_EQ(
                expectedEndOfMessage, 
                writeResult.DataRange.End);

            uint8_t lastChecksumByte;
            {
                auto readableExtent = co_await extentStore->OpenExtentForRead(MakeLogExtentName(0));
                auto readBuffer = co_await readableExtent->Read(expectedEndOfMessage - 1, 1);

                google::protobuf::io::CodedInputStream inputStream(
                    reinterpret_cast<const uint8_t*>(readBuffer.data().data()),
                    readBuffer.data().size());
                inputStream.ReadRaw(
                    &lastChecksumByte,
                    sizeof(lastChecksumByte));
            }

            uint8_t corruptedLastChecksumByte = lastChecksumByte ^ 1;

            {
                auto writableExtent = co_await extentStore->OpenExtentForWrite(MakeLogExtentName(0));
                auto writeBuffer = co_await writableExtent->CreateWriteBuffer();
                auto rawData = co_await writeBuffer->Write(expectedEndOfMessage - 1, 1);
                google::protobuf::io::ArrayOutputStream outputStream(
                    rawData.data().data(),
                    rawData.data().size());

                {
                    google::protobuf::io::CodedOutputStream corruptingOutputStream(
                        &outputStream);

                    corruptingOutputStream.WriteRaw(
                        &corruptedLastChecksumByte,
                        sizeof(corruptedLastChecksumByte));
                }

                co_await writeBuffer->Flush();
            }

            auto randomMessageReader = co_await messageStore->OpenExtentForRandomReadAccess(MakeLogExtentName(0));

            MessageStoreTestMessage actualMessage;

            EXPECT_THROW(
                co_await randomMessageReader->Read(
                    offset,
                    actualMessage),
                std::range_error);
        });
    }
}