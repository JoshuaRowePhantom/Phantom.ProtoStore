#include "StandardIncludes.h"

#include "Phantom.ProtoStore/src/MessageStoreImpl.h"
#include "Phantom.ProtoStore/src/MemoryExtentStore.h"
#include "ProtoStoreTest.pb.h"
#include "src/ProtoStoreInternal_generated.h"
#include "ProtoStoreTest_generated.h"

namespace Phantom::ProtoStore
{
TEST(RandomReaderWriterTest, is_aligned_works_on_all_supported_values)
{
    struct data
    {
        ExtentOffset offset;
        uint8_t alignment;
        bool expectedResult;
    };

    std::vector<data> testData =
    {
        data { 0, 1, true },
        data { 1, 1, true },
        data { 2, 1, true },

        data { 0, 2, true },
        data { 1, 2, false },
        data { 2, 2, true },

        data { 0, 4, true },
        data { 1, 4, false },
        data { 2, 4, false },
        data { 3, 4, false },
        data { 4, 4, true },

        data { 0, 8, true },
        data { 1, 8, false },
        data { 2, 8, false },
        data { 3, 8, false },
        data { 4, 8, false },
        data { 5, 8, false },
        data { 6, 8, false },
        data { 7, 8, false },
        data { 8, 8, true },

        data {  0, 16, true },
        data {  1, 16, false },
        data {  2, 16, false },
        data {  3, 16, false },
        data {  4, 16, false },
        data {  5, 16, false },
        data {  6, 16, false },
        data {  7, 16, false },
        data {  8, 16, false },
        data {  9, 16, false },
        data { 10, 16, false },
        data { 11, 16, false },
        data { 12, 16, false },
        data { 13, 16, false },
        data { 14, 16, false },
        data { 15, 16, false },
        data { 16, 16, true },

        data {  0, 32, true },
        data {  1, 32, false },
        data {  2, 32, false },
        data {  3, 32, false },
        data {  4, 32, false },
        data {  5, 32, false },
        data {  6, 32, false },
        data {  7, 32, false },
        data {  8, 32, false },
        data {  9, 32, false },
        data { 10, 32, false },
        data { 11, 32, false },
        data { 12, 32, false },
        data { 13, 32, false },
        data { 14, 32, false },
        data { 15, 32, false },
        data { 16, 32, false },
        data { 17, 32, false },
        data { 18, 32, false },
        data { 19, 32, false },
        data { 20, 32, false },
        data { 21, 32, false },
        data { 22, 32, false },
        data { 23, 32, false },
        data { 24, 32, false },
        data { 25, 32, false },
        data { 26, 32, false },
        data { 27, 32, false },
        data { 28, 32, false },
        data { 29, 32, false },
        data { 30, 32, false },
        data { 31, 32, false },
        data { 32, 32, true },
        
        data { 0xffffffff00000020,  1, true },
        data { 0xffffffff00000021,  1, true },
        data { 0xffffffff00000020, 32, true },
        data { 0xffffffff00000021, 32, false },
    };

    for (auto test : testData)
    {
        EXPECT_EQ(
            test.expectedResult, 
            RandomMessageReaderWriterBase::is_aligned(test.offset, test.alignment));
    }
}

TEST(RandomReaderWriterTest, align_aligns_upward_for_supported_values)
{
    struct data
    {
        ExtentOffset offset;
        uint8_t alignment;
        ExtentOffset expectedResult;
    };

    std::vector<data> testData =
    {
        data { 0, 1, 0 },
        data { 1, 1, 1 },
        data { 2, 1, 2 },

        data { 0, 2, 0 },
        data { 1, 2, 2 },
        data { 2, 2, 2 },

        data { 0, 4, 0 },
        data { 1, 4, 4 },
        data { 2, 4, 4 },
        data { 3, 4, 4 },
        data { 4, 4, 4 },
        data { 5, 4, 8 },

        data { 0, 8, 0 },
        data { 1, 8, 8 },
        data { 2, 8, 8 },
        data { 3, 8, 8 },
        data { 4, 8, 8 },
        data { 5, 8, 8 },
        data { 6, 8, 8 },
        data { 7, 8, 8 },
        data { 8, 8, 8 },
        data { 9, 8, 16 },

        data {  0, 16,  0},
        data {  1, 16, 16 },
        data {  2, 16, 16 },
        data {  3, 16, 16 },
        data {  4, 16, 16 },
        data {  5, 16, 16 },
        data {  6, 16, 16 },
        data {  7, 16, 16 },
        data {  8, 16, 16 },
        data {  9, 16, 16 },
        data { 10, 16, 16 },
        data { 11, 16, 16 },
        data { 12, 16, 16 },
        data { 13, 16, 16 },
        data { 14, 16, 16 },
        data { 15, 16, 16 },
        data { 16, 16, 16 },
        data { 17, 16, 32 },

        data {  0, 32,  0 },
        data {  1, 32, 32 },
        data {  2, 32, 32 },
        data {  3, 32, 32 },
        data {  4, 32, 32 },
        data {  5, 32, 32 },
        data {  6, 32, 32 },
        data {  7, 32, 32 },
        data {  8, 32, 32 },
        data {  9, 32, 32 },
        data { 10, 32, 32 },
        data { 11, 32, 32 },
        data { 12, 32, 32 },
        data { 13, 32, 32 },
        data { 14, 32, 32 },
        data { 15, 32, 32 },
        data { 16, 32, 32 },
        data { 17, 32, 32 },
        data { 18, 32, 32 },
        data { 19, 32, 32 },
        data { 20, 32, 32 },
        data { 21, 32, 32 },
        data { 22, 32, 32 },
        data { 23, 32, 32 },
        data { 24, 32, 32 },
        data { 25, 32, 32 },
        data { 26, 32, 32 },
        data { 27, 32, 32 },
        data { 28, 32, 32 },
        data { 29, 32, 32 },
        data { 30, 32, 32 },
        data { 31, 32, 32 },
        data { 32, 32, 32 },
        data { 33, 32, 64 },

        data { 0xffffffff00000020,  1, 0xffffffff00000020 },
        data { 0xffffffff00000021,  1, 0xffffffff00000021 },
        data { 0xffffffff00000020, 32, 0xffffffff00000020 },
        data { 0xffffffff00000021, 32, 0xffffffff00000040 },
    };

    for (auto test : testData)
    {
        EXPECT_EQ(
            test.expectedResult,
            RandomMessageReaderWriterBase::align(test.offset, test.alignment));
    }
}

ASYNC_TEST(RandomReaderWriterTest, Open_for_write_writes_expected_header)
{
    auto extentStore = make_shared<MemoryExtentStore>(
        Schedulers::Default());
    auto messageStore = make_shared<MessageStore>(
        Schedulers::Default(),
        extentStore);
    auto randomMessageWriter = co_await messageStore->OpenExtentForRandomWriteAccess(MakeLogExtentName(0));

    auto extent = co_await extentStore->OpenExtentForRead(MakeLogExtentName(0));
    auto header = co_await extent->Read(
        0,
        0x14
    );

    std::vector<char> expectedHeaderContent =
    {
        // Message size
        0x10, 0, 0, 0,
        // Root offset
        0x0c, 0, 0, 0,
        // File identifier
        'P', 'S', 'E', 'X',
        4, 0, 4, 0, 4, 0,
        0, 0
    };

    std::vector<byte> actualHeaderContent(
        header->begin(), 
        header->end()
    );

    EXPECT_TRUE(std::ranges::equal(
        std::as_bytes(std::span{ expectedHeaderContent }),
        actualHeaderContent));
}

ASYNC_TEST(RandomReaderWriterTest, Can_read_what_was_written)
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
}

ASYNC_TEST(RandomReaderWriterTest, Can_write_un_enveloped_FlatBuffer_and_read_it_back_with_envelope)
{
    auto extentStore = make_shared<MemoryExtentStore>(
        Schedulers::Default());
    auto messageStore = make_shared<MessageStore>(
        Schedulers::Default(),
        extentStore);
    auto randomMessageWriter = co_await messageStore->OpenExtentForRandomWriteAccess(MakeLogExtentName(0));

    FlatBuffers::ScalarTableT expectedMessage;
    expectedMessage.item = 5;

    flatbuffers::FlatBufferBuilder builder;
    auto scalarOffset = FlatBuffers::CreateScalarTable(builder, &expectedMessage);
    builder.Finish(scalarOffset);

    auto writeBuffer = co_await randomMessageWriter->WriteFlatMessage(
        0,
        FlatMessage<FlatBuffers::ScalarTable>(builder).data(),
        FlushBehavior::Flush);

    FlatBuffers::ScalarTableT scalar;

    auto randomMessageReader = co_await messageStore->OpenExtentForRandomReadAccess(MakeLogExtentName(0));

    auto flatMessage = FlatMessage<FlatBuffers::ScalarTable>(co_await randomMessageReader->ReadFlatMessage(
        0));
    FlatBuffers::ScalarTableT actualMessage;
    flatMessage->UnPackTo(&actualMessage);

    EXPECT_EQ(
        expectedMessage,
        actualMessage);
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
            writeResult1->DataRange.End,
            expectedMessage2,
            FlushBehavior::Flush);

        auto randomMessageReader = co_await messageStore->OpenExtentForRandomReadAccess(MakeLogExtentName(0));

        MessageStoreTestMessage actualMessage1;
        MessageStoreTestMessage actualMessage2;

        auto readResult1 = co_await randomMessageReader->Read(
            0,
            actualMessage1);

        auto readResult2 = co_await randomMessageReader->Read(
            readResult1->DataRange.End,
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
        size_t expectedEndOfMessage = 524;

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

        EXPECT_EQ(expectedEndOfMessage, writeResult->DataRange.End);

        auto randomMessageReader = co_await messageStore->OpenExtentForRandomReadAccess(MakeLogExtentName(0));

        MessageStoreTestMessage actualMessage;

        auto readResult = co_await randomMessageReader->Read(
            offset,
            actualMessage);

        EXPECT_TRUE(MessageDifferencer::Equals(
            expectedMessage,
            actualMessage));

        EXPECT_EQ(expectedEndOfMessage, readResult->DataRange.End);
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
        size_t expectedEndOfMessage = 524;

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
            writeResult->DataRange.End);

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