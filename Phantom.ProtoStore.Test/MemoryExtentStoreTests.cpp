#include "StandardIncludes.h"

#include "Phantom.ProtoStore/src/MemoryExtentStore.h"
#include <google/protobuf/io/coded_stream.h>

namespace Phantom::ProtoStore
{
using google::protobuf::io::CodedOutputStream;
using google::protobuf::io::CodedInputStream;

TEST(MemoryExtentStoreTests, OpenExtentForRead_succeeds_on_NonExistentExtent)
{
    run_async([]() -> task<>
    {
        MemoryExtentStore store(Schedulers::Default());
        auto extent = co_await store.OpenExtentForRead(MakeLogExtentName(0));
    }
    );
}

TEST(MemoryExtentStoreTests, OpenExtentForRead_cannot_read_past_end_of_zero_length_extent)
{
    run_async([]() -> task<>
    {
        MemoryExtentStore store(Schedulers::Default());
        auto extent = co_await store.OpenExtentForRead(MakeLogExtentName(0));
        EXPECT_THROW(
            (co_await extent->Read(
                0,
                1))
            ,
            std::range_error);
    }
    );
}

TEST(MemoryExtentStoreTests, OpenExtentForRead_can_read_data_written_by_OpenExtentForWrite)
{
    run_async([]() -> task<>
    {
        MemoryExtentStore store(Schedulers::Default());
        vector<uint8_t> expectedData = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        auto writeExtent = co_await store.OpenExtentForWrite(MakeLogExtentName(0));
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

        auto readExtent = co_await store.OpenExtentForRead(MakeLogExtentName(0));
        auto readBuffer = co_await readExtent->Read(0, expectedData.size());
        
        google::protobuf::io::ArrayInputStream stream(
            readBuffer.data().data(),
            readBuffer.data().size()
        );

        CodedInputStream readStream(
            &stream);
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

TEST(MemoryExtentStoreTests, OpenExtentForWrite_can_do_Flush_after_grow)
{
    run_async([]() -> task<>
    {
        MemoryExtentStore store(Schedulers::Default());
        std::basic_string<uint8_t> writeData1(50, '1');
        std::basic_string<uint8_t> writeData2(500, '2');

        auto writeExtent = co_await store.OpenExtentForWrite(MakeLogExtentName(0));
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

        auto readExtent = co_await store.OpenExtentForRead(MakeLogExtentName(0));
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
    );
}

TEST(MemoryExtentStoreTests, DeleteExtent_erases_the_content)
{
    run_async([]() -> task<>
    {
        MemoryExtentStore store(Schedulers::Default());
        vector<uint8_t> writeData = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

        {
            auto writeExtent = co_await store.OpenExtentForWrite(MakeLogExtentName(0));
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

        co_await store.DeleteExtent(MakeLogExtentName(0));

        auto extent = co_await store.OpenExtentForRead(MakeLogExtentName(0));

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
