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
        co_await writeBuffer->Write(0, expectedData.size());

        {
            CodedOutputStream writeStream(writeBuffer->Stream());
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
        co_await writeBuffer1->Write(0, writeData1.size());

        auto writeBuffer2 = co_await writeExtent->CreateWriteBuffer();
        co_await writeBuffer2->Write(writeData1.size(), writeData2.size());

        auto writeBuffer3 = co_await writeExtent->CreateWriteBuffer();
        co_await writeBuffer3->Write(writeData1.size() + writeData2.size(), writeData3.size());

        {
            CodedOutputStream writeStream(writeBuffer1->Stream());
            writeStream.WriteRaw(
                writeData1.data(),
                writeData1.size());
        }

        {
            CodedOutputStream writeStream(writeBuffer2->Stream());
            writeStream.WriteRaw(
                writeData2.data(),
                writeData2.size());
        }

        {
            CodedOutputStream writeStream(writeBuffer3->Stream());
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
            co_await writeBuffer->Write(0, writeData.size());

            {
                CodedOutputStream writeStream(writeBuffer->Stream());
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
