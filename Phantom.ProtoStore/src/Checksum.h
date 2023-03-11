#pragma once

#include "StandardTypes.h"
#include <boost/crc.hpp>

namespace Phantom::ProtoStore
{
uint32_t checksum_v1(
    std::span<const byte>
);

uint64_t hash_v1(
    std::span<const byte> data
);

using crc_v1_type = boost::crc_optimal<32, 0x1EDC6F41, 0xffffffff, 0, true, true>;
using crc_hash_v1_type = boost::crc_optimal<64, 0x42F0E1EBA9EA3693, 0xffffffff, 0, true, true>;

}
