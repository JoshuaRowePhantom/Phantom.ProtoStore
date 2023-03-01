#pragma once

#include "StandardIncludes.h"

namespace Phantom::ProtoStore
{
class ExtentStoreTests
{
public:
    static task<> OpenExtentForRead_succeeds_on_NonExistentExtent(
        IExtentStore& store
    );
    
    static task<> OpenExtentForRead_cannot_read_past_end_of_zero_length_extent(
        IExtentStore& store
    );

    static task<> OpenExtentForRead_can_read_data_written_by_OpenExtentForWrite(
        IExtentStore& store
    );

    static task<> OpenExtentForWrite_can_do_Flush_after_grow(
        IExtentStore& store
    );

    static task<> DeleteExtent_erases_the_content(
        IExtentStore& store
    );

    static task<> DeleteExtent_erases_the_content_while_a_DataReference_exists(
        IExtentStore& store
    );

    static task<> Data_is_readable_after_Commit_and_Flush(
        IExtentStore& store
    );

    static task<> Can_extend_extent_while_data_reference_is_held(
        IExtentStore& store
    );
};
}