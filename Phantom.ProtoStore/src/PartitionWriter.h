#pragma once

#include "StandardTypes.h"
#include "MemoryTable.h"
#include <cppcoro/async_generator.hpp>

namespace Phantom::ProtoStore
{
struct WriteRowsRequest
{
    size_t approximateRowCount;
    // This would be a row_generator, but Msvc gives an ICE.
    row_generator* rows;
    ExtentOffset inputSize;
    // The target size for the entire extent. This is a soft limit.
    ExtentOffset targetExtentSize = std::numeric_limits<ExtentOffset>::max();
    // The target size for an individual message.
    ExtentOffset targetMessageSize = std::numeric_limits<ExtentOffset>::max();
};

struct WriteRowsResult
{
    size_t rowsIterated;
    size_t rowsWritten;
    ExtentOffset writtenDataSize;
    ExtentOffset writtenExtentSize;
    row_generator_iterator resumptionRow;
    SequenceNumber latestSequenceNumber = SequenceNumber::Earliest;
    SequenceNumber earliestSequenceNumber = SequenceNumber::Latest;
};

class IPartitionWriter
{
public:
    virtual task<WriteRowsResult> WriteRows(
        WriteRowsRequest& writeRowsRequest
    ) = 0;
};
}
