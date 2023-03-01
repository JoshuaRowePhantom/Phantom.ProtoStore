#include "StandardIncludes.h"

#include "Phantom.ProtoStore/src/MemoryExtentStore.h"
#include "ExtentStoreTests.h"
#include <google/protobuf/io/coded_stream.h>

namespace Phantom::ProtoStore
{
using google::protobuf::io::CodedOutputStream;
using google::protobuf::io::CodedInputStream;

ASYNC_TEST(MemoryExtentStoreTests, OpenExtentForRead_succeeds_on_NonExistentExtent)
{
    MemoryExtentStore store(Schedulers::Default());
    co_await ExtentStoreTests::OpenExtentForRead_succeeds_on_NonExistentExtent(store);
}

ASYNC_TEST(MemoryExtentStoreTests, OpenExtentForRead_cannot_read_past_end_of_zero_length_extent)
{
    MemoryExtentStore store(Schedulers::Default());
    co_await ExtentStoreTests::OpenExtentForRead_cannot_read_past_end_of_zero_length_extent(store);
}

ASYNC_TEST(MemoryExtentStoreTests, OpenExtentForRead_can_read_data_written_by_OpenExtentForWrite)
{
    MemoryExtentStore store(Schedulers::Default());
    co_await ExtentStoreTests::OpenExtentForRead_can_read_data_written_by_OpenExtentForWrite(store);
}

ASYNC_TEST(MemoryExtentStoreTests, OpenExtentForWrite_can_do_Flush_after_grow)
{
    MemoryExtentStore store(Schedulers::Default());
    co_await ExtentStoreTests::OpenExtentForWrite_can_do_Flush_after_grow(store);
}
ASYNC_TEST(MemoryExtentStoreTests, DeleteExtent_erases_the_content)
{
    MemoryExtentStore store(Schedulers::Default());
    co_await ExtentStoreTests::DeleteExtent_erases_the_content(store);
}

ASYNC_TEST(MemoryExtentStoreTests, DeleteExtent_erases_the_content_while_a_DataReference_exists)
{
    MemoryExtentStore store(Schedulers::Default());
    co_await ExtentStoreTests::DeleteExtent_erases_the_content_while_a_DataReference_exists(store);
}

ASYNC_TEST(MemoryExtentStoreTests, Data_is_readable_after_Commit_and_Flush)
{
    MemoryExtentStore store(Schedulers::Default());
    co_await ExtentStoreTests::Data_is_readable_after_Commit_and_Flush(store);
}

ASYNC_TEST(MemoryExtentStoreTests, Can_extend_extent_while_data_reference_is_held)
{
    MemoryExtentStore store(Schedulers::Default());
    co_await ExtentStoreTests::Can_extend_extent_while_data_reference_is_held(store);
}

ASYNC_TEST(MemoryExtentStoreTests, Open_extent_for_write_erases_previous_content)
{
    MemoryExtentStore store(Schedulers::Default());
    co_await ExtentStoreTests::Open_extent_for_write_erases_previous_content(store);
}

}
