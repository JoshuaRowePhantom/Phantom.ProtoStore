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

    typedef cppcoro::async_generator<const MemoryTableRow*> row_generator;

    cppcoro::async_generator<const MemoryTableRow*> Merge(
        cppcoro::generator<row_generator> rowSources
    );
};

}
