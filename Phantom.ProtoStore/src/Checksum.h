#pragma once

#include "StandardTypes.h"
#include "Phantom.System/crc32.h"

namespace Phantom::ProtoStore
{
uint32_t checksum_v1(
    std::span<const byte>
);

uint64_t hash_v1(
    std::span<const byte> data
);

using hash_v1_type = Phantom::buffered_crc<128, Phantom::crc32<0xffffffff, 0>>;

}
