#pragma once

#include "StandardTypes.h"
#include "MemoryTable.h"
#include <cppcoro/generator.hpp>
#include <cppcoro/async_generator.hpp>

namespace Phantom::ProtoStore
{

class RowMerger
{
    KeyComparer* m_keyComparer;
public:
    RowMerger(
        KeyComparer* keyComparer
    );

    // Merge multiple row sources into a single row source.
    // The inputs must be sorted by key, then WriteSequenceNubmer descending.
    // The result of the merge is a sequence of rows sorted by
    // key and then WriteSequenceNumber descending.
    row_generator Merge(
        row_generators rowSources
    );

    // Merge multiple row sources into a single row source
    // that is the result of a call to Enumerate.
    // The inputs must be sorted by key, then WriteSequenceNubmer descending.
    // The result of the merge is a sequence of rows sorted by
    // key, only the most recent WriteSequenceNumber appearing,
    // and deleted rows skipped.
    row_generator Enumerate(
        row_generators rowSources
    );
};

}
