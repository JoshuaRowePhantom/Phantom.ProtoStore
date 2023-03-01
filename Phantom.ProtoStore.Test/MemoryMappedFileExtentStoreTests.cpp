#include "StandardIncludes.h"

#include "ExtentStoreTests.h"
#include "Phantom.ProtoStore/src/MemoryMappedFileExtentStore.h"
#include <codecvt>
#include <filesystem>
#include <google/protobuf/io/coded_stream.h>
#include <locale>

namespace Phantom::ProtoStore
{
using google::protobuf::io::CodedOutputStream;
using google::protobuf::io::CodedInputStream;
using google::protobuf::io::ArrayOutputStream;

ASYNC_TEST(MemoryMappedFileExtentStoreTests, OpenExtentForRead_succeeds_on_NonExistentExtent)
{
    auto store = MakeFilesystemStore(
        "MemoryMappedFileExtentStoreTests",
        "OpenExtentForRead_succeeds_on_NonExistentExtent",
        4096);
    co_await ExtentStoreTests::OpenExtentForRead_succeeds_on_NonExistentExtent(*store);
}

ASYNC_TEST(MemoryMappedFileExtentStoreTests, OpenExtentForRead_cannot_read_past_end_of_zero_length_extent)
{
    auto store = MakeFilesystemStore(
        "MemoryMappedFileExtentStoreTests",
        "OpenExtentForRead_succeeds_on_NonExistentExtent",
        4096);
    co_await ExtentStoreTests::OpenExtentForRead_cannot_read_past_end_of_zero_length_extent(*store);
}

ASYNC_TEST(MemoryMappedFileExtentStoreTests, OpenExtentForRead_can_read_data_written_by_OpenExtentForWrite)
{
    auto store = MakeFilesystemStore(
        "MemoryMappedFileExtentStoreTests",
        "OpenExtentForRead_can_read_data_written_by_OpenExtentForWrite",
        4096);
    co_await ExtentStoreTests::OpenExtentForRead_can_read_data_written_by_OpenExtentForWrite(*store);
}

ASYNC_TEST(MemoryMappedFileExtentStoreTests, OpenExtentForWrite_can_do_Flush_after_grow)
{
    auto store = MakeFilesystemStore(
        "MemoryMappedFileExtentStoreTests",
        "OpenExtentForWrite_can_do_Flush_after_grow",
        4096);
    co_await ExtentStoreTests::OpenExtentForWrite_can_do_Flush_after_grow(*store);
}

ASYNC_TEST(MemoryMappedFileExtentStoreTests, DeleteExtent_erases_the_content)
{
    auto store = MakeFilesystemStore(
        "MemoryMappedFileExtentStoreTests",
        "DeleteExtent_erases_the_content",
        4096);
    co_await ExtentStoreTests::DeleteExtent_erases_the_content(*store);
}

ASYNC_TEST(MemoryMappedFileExtentStoreTests, DeleteExtent_erases_the_content_while_a_DataReference_exists)
{
    auto store = MakeFilesystemStore(
        "MemoryMappedFileExtentStoreTests",
        "DeleteExtent_erases_the_content_while_a_DataReference_exists",
        4096);
    co_await ExtentStoreTests::DeleteExtent_erases_the_content_while_a_DataReference_exists(*store);
}

ASYNC_TEST(MemoryMappedFileExtentStoreTests, Data_is_readable_after_Commit_and_Flush)
{
    auto store = MakeFilesystemStore(
        "MemoryMappedFileExtentStoreTests",
        "DeleteExtent_erases_the_content_while_a_DataReference_exists",
        4096);
    co_await ExtentStoreTests::Data_is_readable_after_Commit_and_Flush(*store);
}

ASYNC_TEST(MemoryMappedFileExtentStoreTests, Can_extend_extent_while_data_reference_is_held)
{
    auto store = MakeFilesystemStore(
        "MemoryMappedFileExtentStoreTests",
        "Can_extend_extent_while_data_reference_is_held",
        4096);
    co_await ExtentStoreTests::Can_extend_extent_while_data_reference_is_held(*store);
}

}
