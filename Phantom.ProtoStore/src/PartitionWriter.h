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
    ExtentOffset targetSize = std::numeric_limits<ExtentOffset>::max();
};

struct WriteRowsResult
{
    size_t rowsIterated;
    size_t rowsWritten;
    ExtentOffset writtenDataSize;
    ExtentOffset writtenExtentSize;
    row_generator_iterator resumptionRow;
};

class IPartitionWriter
{
public:
    virtual task<WriteRowsResult> WriteRows(
        WriteRowsRequest writeRowsRequest
    ) = 0;
};
}
