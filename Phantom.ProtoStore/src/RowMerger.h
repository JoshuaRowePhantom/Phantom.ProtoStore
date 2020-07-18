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

    row_generator Merge(
        row_generators rowSources
    );
};

}
