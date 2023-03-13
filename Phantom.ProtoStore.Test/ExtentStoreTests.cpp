#include "ExtentStoreTests.h"
#include "async_test.h"
#include "Phantom.ProtoStore/src/ExtentStore.h"
#include <google/protobuf/io/coded_stream.h>

namespace Phantom::ProtoStore
{
using google::protobuf::io::CodedOutputStream;
using google::protobuf::io::CodedInputStream;


task<> ExtentStoreTests::OpenExtentForRead_succeeds_on_NonExistentExtent(
    IExtentStore& store
)
{
    auto extent = co_await store.OpenExtentForRead(FlatValue(MakeLogExtentName(0)));
}

task<> ExtentStoreTests::OpenExtentForRead_cannot_read_past_end_of_zero_length_extent(
    IExtentStore& store
)
{
    auto extent = co_await store.OpenExtentForRead(FlatValue(MakeLogExtentName(0)));
    EXPECT_EQ(nullptr, (co_await extent->Read(
        0,
        1))->data());
}

task<> ExtentStoreTests::OpenExtentForRead_can_read_data_written_by_OpenExtentForWrite(
    IExtentStore& store
)
{
    vector<uint8_t> expectedData = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    auto writeExtent = co_await store.OpenExtentForWrite(FlatValue(MakeLogExtentName(0)));
    auto writeBuffer = co_await writeExtent->CreateWriteBuffer();
    auto rawData = co_await writeBuffer->Write(0, expectedData.size());
    google::protobuf::io::ArrayOutputStream outputStream(
        rawData.data().data(),
        rawData.data().size());

    {
        CodedOutputStream writeStream(&outputStream);
        writeStream.WriteRaw(
            expectedData.data(),
            expectedData.size());
    }
    co_await writeBuffer->Flush();

    auto readExtent = co_await store.OpenExtentForRead(FlatValue(MakeLogExtentName(0)));
    auto readBuffer = co_await readExtent->Read(0, expectedData.size());
    CodedInputStream readStream(
        reinterpret_cast<const uint8_t*>(readBuffer.data().data()),
        readBuffer.data().size());
    vector<uint8_t> actualData(expectedData.size());
    readStream.ReadRaw(
        actualData.data(),
        actualData.size());

    EXPECT_EQ(
        expectedData,
        actualData);
}

task<> ExtentStoreTests::OpenExtentForWrite_can_do_Flush_after_grow(IExtentStore& store)
{
    std::basic_string<uint8_t> writeData1(50, '1');
    std::basic_string<uint8_t> writeData2(500, '2');

    auto writeExtent = co_await store.OpenExtentForWrite(FlatValue(MakeLogExtentName(0)));
    auto writeBuffer1 = co_await writeExtent->CreateWriteBuffer();
    auto rawData1 = co_await writeBuffer1->Write(0, writeData1.size());
    google::protobuf::io::ArrayOutputStream outputStream1(
        rawData1.data().data(),
        rawData1.data().size());

    auto writeBuffer2 = co_await writeExtent->CreateWriteBuffer();
    auto rawData2 = co_await writeBuffer2->Write(writeData1.size(), writeData2.size());
    google::protobuf::io::ArrayOutputStream outputStream2(
        rawData2.data().data(),
        rawData2.data().size());

    {
        CodedOutputStream writeStream(&outputStream1);
        writeStream.WriteRaw(
            writeData1.data(),
            writeData1.size());
    }

    {
        CodedOutputStream writeStream(&outputStream2);
        writeStream.WriteRaw(
            writeData2.data(),
            writeData2.size());
    }

    co_await writeBuffer2->Flush();
    co_await writeBuffer1->Flush();

    auto expectedData = writeData1 + writeData2;

    auto readExtent = co_await store.OpenExtentForRead(FlatValue(MakeLogExtentName(0)));
    auto readBuffer = co_await readExtent->Read(0, expectedData.size());
    CodedInputStream readStream(
        reinterpret_cast<const uint8_t*>(readBuffer.data().data()),
        readBuffer.data().size());
    std::basic_string<uint8_t> actualData(expectedData.size(), '3');
    readStream.ReadRaw(
        actualData.data(),
        actualData.size());

    EXPECT_EQ(
        expectedData,
        actualData);
}

task<> ExtentStoreTests::DeleteExtent_erases_the_content(
    IExtentStore& store
)
{
    vector<uint8_t> writeData = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

    {
        auto writeExtent = co_await store.OpenExtentForWrite(FlatValue(MakeLogExtentName(0)));
        auto writeBuffer = co_await writeExtent->CreateWriteBuffer();
        auto rawData = co_await writeBuffer->Write(0, writeData.size());
        
        std::copy_n(
            reinterpret_cast<std::byte*>(writeData.data()),
            writeData.size(),
            rawData->begin()
        );

        co_await writeBuffer->Flush();
    }

    co_await store.DeleteExtent(FlatValue(MakeLogExtentName(0)));

    auto extent = co_await store.OpenExtentForRead(FlatValue(MakeLogExtentName(0)));

    EXPECT_EQ(nullptr, (co_await extent->Read(
        0,
        1))->data());
}


task<> ExtentStoreTests::DeleteExtent_erases_the_content_while_a_DataReference_exists(
    IExtentStore& store
)
{
    vector<uint8_t> writeData = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

    WritableRawData rawData;

    {
        auto writeExtent = co_await store.OpenExtentForWrite(FlatValue(MakeLogExtentName(0)));
        auto writeBuffer = co_await writeExtent->CreateWriteBuffer();
        rawData = co_await writeBuffer->Write(0, writeData.size());

        std::copy_n(
            reinterpret_cast<std::byte*>(writeData.data()),
            writeData.size(),
            rawData->begin()
        );

        co_await writeBuffer->Flush();
    }

    co_await store.DeleteExtent(FlatValue(MakeLogExtentName(0)));

    auto extent = co_await store.OpenExtentForRead(FlatValue(MakeLogExtentName(0)));

    EXPECT_EQ(nullptr, (co_await extent->Read(
        0,
        1))->data());

    EXPECT_TRUE(
        std::ranges::equal(
            get_uint8_t_span(*rawData),
            writeData
        ));
}

task<> ExtentStoreTests::Data_is_readable_after_Commit_and_Flush(
    IExtentStore& store
)
{
    vector<uint8_t> writeData = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    auto expectedData = as_bytes(std::span<uint8_t>{ writeData });

    WritableRawData rawData;

    auto writeExtent = co_await store.OpenExtentForWrite(FlatValue(MakeLogExtentName(0)));
    auto writeBuffer = co_await writeExtent->CreateWriteBuffer();
    rawData = co_await writeBuffer->Write(0, writeData.size());

    std::copy_n(
        reinterpret_cast<std::byte*>(writeData.data()),
        writeData.size(),
        rawData->begin()
    );

    co_await writeBuffer->Commit();
    EXPECT_TRUE(std::ranges::equal(expectedData, *rawData));
    co_await writeBuffer->Flush();
    EXPECT_TRUE(std::ranges::equal(expectedData, *rawData));
}

task<> ExtentStoreTests::Can_extend_extent_while_data_reference_is_held(
    IExtentStore& store
)
{
    vector<uint8_t> writeData1 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    vector<uint8_t> writeData2 = { 2, 2, 3, 4, 5, 6, 7, 8, 9, 20 };
    auto expectedData1 = as_bytes(std::span<uint8_t>{ writeData1 });
    auto expectedData2 = as_bytes(std::span<uint8_t>{ writeData2 });

    WritableRawData rawData1;
    WritableRawData rawData2;

    auto writeExtent = co_await store.OpenExtentForWrite(FlatValue(MakeLogExtentName(0)));
    auto writeBuffer1 = co_await writeExtent->CreateWriteBuffer();
    rawData1 = co_await writeBuffer1->Write(0, writeData1.size());
    std::ranges::copy(expectedData1, rawData1->data());

    auto writeBuffer2 = co_await writeExtent->CreateWriteBuffer();
    rawData2 = co_await writeBuffer2->Write(65536, writeData2.size());
    std::ranges::copy(expectedData2, rawData2->data());

    auto readExtent = co_await store.OpenExtentForRead(FlatValue(MakeLogExtentName(0)));

    co_await writeBuffer1->Commit();
    co_await writeBuffer2->Flush();

    EXPECT_TRUE(std::ranges::equal(expectedData1, *rawData1));
    EXPECT_TRUE(std::ranges::equal(expectedData2, *rawData2));

    auto actualData1 = co_await readExtent->Read(0, expectedData1.size());
    auto actualData2 = co_await readExtent->Read(65536, expectedData1.size());

    EXPECT_TRUE(std::ranges::equal(expectedData1, *actualData1));
    EXPECT_TRUE(std::ranges::equal(expectedData2, *actualData2));
}

task<> ExtentStoreTests::Open_extent_for_write_erases_previous_content(
    IExtentStore& store
)
{
    vector<uint8_t> writeData1 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    auto writeDataExtent1 = as_bytes(std::span<uint8_t>{ writeData1 });
    auto writeExtent = co_await store.OpenExtentForWrite(FlatValue(MakeLogExtentName(0)));
    auto writeBuffer1 = co_await writeExtent->CreateWriteBuffer();
    auto rawData1 = co_await writeBuffer1->Write(0, writeData1.size());
    std::ranges::copy(writeDataExtent1, rawData1->data());
    co_await writeBuffer1->Flush();

    writeExtent = co_await store.OpenExtentForWrite(FlatValue(MakeLogExtentName(0)));

    auto readExtent = co_await store.OpenExtentForRead(FlatValue(MakeLogExtentName(0)));
    auto actualData1 = co_await readExtent->Read(0, writeData1.size());
    EXPECT_TRUE(!actualData1->data());
    EXPECT_EQ(0, actualData1->size());
}

}
