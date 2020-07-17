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
    typedef cppcoro::generator<row_generator> row_generators;

    row_generator Merge(
        row_generators rowSources
    );
};

}
