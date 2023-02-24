#pragma once

#include "StandardTypes.h"
#include <google/protobuf/io/zero_copy_stream.h>

namespace Phantom::ProtoStore
{
uint32_t checksum_v1(
    std::span<const byte>
);

}
