#include "Phantom.ProtoStore/src/MemoryExtentStore.h"
#include <cppcoro/sync_wait.hpp>
#include <gtest/gtest.h>
#include "Phantom.System/utility.h"

namespace Phantom::ProtoStore
{
    using google::protobuf::io::CodedOutputStream;
    using google::protobuf::io::CodedInputStream;
    using namespace Phantom::System;

    TEST(MemoryExtentStoreTests, OpenExtentForRead_succeeds_on_NonExistentExtent)
    {
        run_async([]() -> task<>
            {
                MemoryExtentStore store;
                auto extent = co_await store.OpenExtentForRead(0);
            }
        );
    }

    TEST(MemoryExtentStoreTests, OpenExtentForRead_cannot_read_past_end_of_zero_length_extent)
    {
        run_async([]() -> task<>
            {
                MemoryExtentStore store;
                auto extent = co_await store.OpenExtentForRead(0);
                ASSERT_THROW(
                    (co_await extent->Read(
                        0,
                        1))
                    ,
                    std::out_of_range);
            }
        );
    }

    TEST(MemoryExtentStoreTests, OpenExtentForRead_can_read_data_written_by_OpenExtentForWrite)
    {
        run_async([]() -> task<>
            {
                MemoryExtentStore store;
                vector<uint8_t> expectedData = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
                auto writeExtent = co_await store.OpenExtentForWrite(0);
                auto writeBuffer = co_await writeExtent->Write(0, expectedData.size());
                
                {
                    CodedOutputStream writeStream(writeBuffer->Stream());
                    writeStream.WriteRaw(
                        expectedData.data(),
                        expectedData.size());
                }
                co_await writeBuffer->Flush();

                auto readExtent = co_await store.OpenExtentForRead(0);
                auto readBuffer = co_await readExtent->Read(0, expectedData.size());
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
}
