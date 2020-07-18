#pragma once

#include "StandardTypes.h"
#include "MemoryTable.h"
#include <cppcoro/async_generator.hpp>

namespace Phantom::ProtoStore
{
class IPartitionWriter
{
public:
    virtual task<> WriteRows(
        size_t rowCount,
        row_generator rows
    ) = 0;
};
}
