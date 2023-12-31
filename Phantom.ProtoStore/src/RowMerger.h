#pragma once

#include "StandardTypes.h"
#include "MemoryTable.h"
#include <cppcoro/generator.hpp>
#include <cppcoro/async_generator.hpp>

namespace Phantom::ProtoStore
{

class RowMerger
{
    const std::shared_ptr<const ValueComparer> m_keyComparer;

public:
    RowMerger(
        std::shared_ptr<const ValueComparer> keyComparer
    );

    // Merge multiple row sources into a single row source.
    // The inputs must be sorted by key, then WriteSequenceNumber descending.
    // The result of the merge is a sequence of rows sorted by
    // key and then WriteSequenceNumber descending.
    row_generator Merge(
        row_generators rowSources
    );

    // Filter rows that are outside the snapshot window
    // during a top-level merge.
    // This keeps the most recent row for any given key,
    // and the most recent row for any given key that is outside
    // the snapshot window.
    // If that row is a delete, that row is dropped.
    row_generator FilterTopLevelMergeSnapshotWindowRows(
        row_generator source,
        SequenceNumber earliestSequenceNumber
    );

    // Merge multiple row sources into a single row source
    // that is the result of a call to Enumerate.
    // The inputs must be sorted by key, then WriteSequenceNumber descending.
    // The result of the merge is a sequence of rows sorted by
    // key, only the most recent WriteSequenceNumber appearing,
    // and deleted rows skipped.
    row_generator Enumerate(
        row_generators rowSources
    );
};

}
