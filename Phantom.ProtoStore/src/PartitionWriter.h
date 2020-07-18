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
        row_generator rows
    ) = 0;
};
}
