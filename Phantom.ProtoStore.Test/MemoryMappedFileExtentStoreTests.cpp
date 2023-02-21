#include "StandardIncludes.h"

#include "Phantom.ProtoStore/src/MemoryMappedFileExtentStore.h"
#include <google/protobuf/io/coded_stream.h>
#include <codecvt>
#include <filesystem>
#include <locale>

namespace Phantom::ProtoStore
{
using google::protobuf::io::CodedOutputStream;
using google::protobuf::io::CodedInputStream;
using google::protobuf::io::ArrayOutputStream;

TEST(MemoryMappedFileExtentStoreTests, OpenExtentForRead_succeeds_on_NonExistentExtent)
{
    run_async([=]() -> task<>
    {
        auto store = MakeFilesystemStore(
            "MemoryMappedFileExtentStoreTests", 
            "OpenExtentForRead_succeeds_on_NonExistentExtent", 
            4096);
        auto extent = co_await store->OpenExtentForRead(MakeLogExtentName(0));
    }
    );
}

TEST(MemoryMappedFileExtentStoreTests, OpenExtentForRead_cannot_read_past_end_of_zero_length_extent)
{
    run_async([=]() -> task<>
    {
        auto store = MakeFilesystemStore(
            "MemoryMappedFileExtentStoreTests",
            "OpenExtentForRead_succeeds_on_NonExistentExtent",
            4096);
        auto extent = co_await store->OpenExtentForRead(MakeLogExtentName(0));
        EXPECT_THROW(
            (co_await extent->Read(
                0,
                1))
            ,
            std::range_error);
    }
    );
}

TEST(MemoryMappedFileExtentStoreTests, OpenExtentForRead_can_read_data_written_by_OpenExtentForWrite)
{
    run_async([=]() -> task<>
    {
        auto store = MakeFilesystemStore(
            "MemoryMappedFileExtentStoreTests",
            "OpenExtentForRead_can_read_data_written_by_OpenExtentForWrite",
            4096);
        vector<uint8_t> expectedData = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        auto writeExtent = co_await store->OpenExtentForWrite(MakeLogExtentName(0));
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

        auto readExtent = co_await store->OpenExtentForRead(MakeLogExtentName(0));
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
    );
}

TEST(MemoryMappedFileExtentStoreTests, OpenExtentForWrite_can_do_Flush_after_grow)
{
    run_async([=]() -> task<>
    {
        auto store = MakeFilesystemStore(
            "MemoryMappedFileExtentStoreTests",
            "OpenExtentForWrite_can_do_Flush_after_grow",
            4096);
        std::basic_string<uint8_t> writeData1(50, '1');
        std::basic_string<uint8_t> writeData2(50, '2');
        std::basic_string<uint8_t> writeData3(5000, '3');

        auto writeExtent = co_await store->OpenExtentForWrite(MakeLogExtentName(0));
        
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

        auto writeBuffer3 = co_await writeExtent->CreateWriteBuffer();
        auto rawData3 = co_await writeBuffer3->Write(writeData1.size() + writeData2.size(), writeData3.size());
        google::protobuf::io::ArrayOutputStream outputStream3(
            rawData3.data().data(),
            rawData3.data().size());

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

        {
            CodedOutputStream writeStream(&outputStream3);
            writeStream.WriteRaw(
                writeData3.data(),
                writeData3.size());
        }

        co_await writeBuffer2->Flush();
        co_await writeBuffer1->Flush();
        co_await writeBuffer3->Flush();

        auto expectedData = writeData1 + writeData2 + writeData3;

        auto readExtent = co_await store->OpenExtentForRead(MakeLogExtentName(0));
        auto readBuffer = co_await readExtent->Read(0, expectedData.size());
        CodedInputStream readStream(
            reinterpret_cast<const uint8_t*>(readBuffer.data().data()),
            readBuffer.data().size());
        std::basic_string<uint8_t> actualData(expectedData.size(), '0');
        readStream.ReadRaw(
            actualData.data(),
            actualData.size());

        EXPECT_EQ(
            expectedData,
            actualData);
    }
    );
}

TEST(MemoryMappedFileExtentStoreTests, DeleteExtent_erases_the_content)
{
    run_async([=]() -> task<>
    {
        auto store = MakeFilesystemStore(
            "MemoryMappedFileExtentStoreTests",
            "DeleteExtent_erases_the_content",
            4096);
        vector<uint8_t> writeData = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

        {
            auto writeExtent = co_await store->OpenExtentForWrite(MakeLogExtentName(0));
            auto writeBuffer = co_await writeExtent->CreateWriteBuffer();
            auto rawData = co_await writeBuffer->Write(0, writeData.size());
            google::protobuf::io::ArrayOutputStream outputStream(
                rawData.data().data(),
                rawData.data().size());

            {
                CodedOutputStream writeStream(&outputStream);
                writeStream.WriteRaw(
                    writeData.data(),
                    writeData.size());
            }
            co_await writeBuffer->Flush();
        }

        co_await store->DeleteExtent(MakeLogExtentName(0));

        auto extent = co_await store->OpenExtentForRead(MakeLogExtentName(0));

        EXPECT_THROW(
            (co_await extent->Read(
                0,
                1))
            ,
            std::range_error);
    }
    );
}
}
