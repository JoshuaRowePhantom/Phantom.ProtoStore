#include "StandardIncludes.h"

#include "Phantom.ProtoStore/src/MemoryMappedFileExtentStore.h"
#include <google/protobuf/io/coded_stream.h>
#include <filesystem>

namespace Phantom::ProtoStore
{
using google::protobuf::io::CodedOutputStream;
using google::protobuf::io::CodedInputStream;

class MemoryMappedFileExtentStoreTests : public testing::Test
{
protected:
    MemoryMappedFileExtentStore MakeEmptyStore();
};

MemoryMappedFileExtentStore MemoryMappedFileExtentStoreTests::MakeEmptyStore()
{
    std::filesystem::remove_all("c:\\mmfst");
    std::filesystem::create_directory("c:\\mmfst");
    return MemoryMappedFileExtentStore("c:\\mmfst\\pref", ".dat", 4096);
}

TEST_F(MemoryMappedFileExtentStoreTests, OpenExtentForRead_succeeds_on_NonExistentExtent)
{
    run_async([=]() -> task<>
    {
        auto store = MakeEmptyStore();
        auto extent = co_await store.OpenExtentForRead(0);
    }
    );
}

TEST_F(MemoryMappedFileExtentStoreTests, OpenExtentForRead_cannot_read_past_end_of_zero_length_extent)
{
    run_async([=]() -> task<>
    {
        auto store = MakeEmptyStore();
        auto extent = co_await store.OpenExtentForRead(0);
        auto buffer = co_await extent->CreateReadBuffer();
        ASSERT_THROW(
            (co_await buffer->Read(
                0,
                1))
            ,
            std::range_error);
    }
    );
}

TEST_F(MemoryMappedFileExtentStoreTests, OpenExtentForRead_can_read_data_written_by_OpenExtentForWrite)
{
    run_async([=]() -> task<>
    {
        auto store = MakeEmptyStore();
        vector<uint8_t> expectedData = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        auto writeExtent = co_await store.OpenExtentForWrite(0);
        auto writeBuffer = co_await writeExtent->CreateWriteBuffer();
        co_await writeBuffer->Write(0, expectedData.size());

        {
            CodedOutputStream writeStream(writeBuffer->Stream());
            writeStream.WriteRaw(
                expectedData.data(),
                expectedData.size());
        }
        co_await writeBuffer->Flush();

        auto readExtent = co_await store.OpenExtentForRead(0);
        auto readBuffer = co_await readExtent->CreateReadBuffer();
        co_await readBuffer->Read(0, expectedData.size());
        CodedInputStream readStream(readBuffer->Stream());
        vector<uint8_t> actualData(expectedData.size());
        readStream.ReadRaw(
            actualData.data(),
            actualData.size());

        ASSERT_EQ(
            expectedData,
            actualData);
    }
    );
}

TEST_F(MemoryMappedFileExtentStoreTests, OpenExtentForWrite_can_do_Flush_after_grow)
{
    run_async([=]() -> task<>
    {
        auto store = MakeEmptyStore();
        std::basic_string<uint8_t> writeData1(50, '1');
        std::basic_string<uint8_t> writeData2(500, '2');

        auto writeExtent = co_await store.OpenExtentForWrite(0);
        auto writeBuffer1 = co_await writeExtent->CreateWriteBuffer();
        co_await writeBuffer1->Write(0, writeData1.size());

        auto writeBuffer2 = co_await writeExtent->CreateWriteBuffer();
        co_await writeBuffer2->Write(writeData1.size(), writeData2.size());

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

        co_await writeBuffer2->Flush();
        co_await writeBuffer1->Flush();

        auto expectedData = writeData1 + writeData2;

        auto readExtent = co_await store.OpenExtentForRead(0);
        auto readBuffer = co_await readExtent->CreateReadBuffer();
        co_await readBuffer->Read(0, expectedData.size());
        CodedInputStream readStream(readBuffer->Stream());
        std::basic_string<uint8_t> actualData(expectedData.size(), '3');
        readStream.ReadRaw(
            actualData.data(),
            actualData.size());

        ASSERT_EQ(
            expectedData,
            actualData);
    }
    );
}

TEST_F(MemoryMappedFileExtentStoreTests, DeleteExtent_erases_the_content)
{
    run_async([=]() -> task<>
    {
        auto store = MakeEmptyStore();
        vector<uint8_t> writeData = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

        {
            auto writeExtent = co_await store.OpenExtentForWrite(0);
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

        co_await store.DeleteExtent(0);

        auto extent = co_await store.OpenExtentForRead(0);
        auto readBuffer = co_await extent->CreateReadBuffer();

        ASSERT_THROW(
            (co_await readBuffer->Read(
                0,
                1))
            ,
            std::range_error);
    }
    );
}
}