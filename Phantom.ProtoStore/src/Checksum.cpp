#include "Checksum.h"

namespace Phantom::ProtoStore
{

uint32_t checksum_v1(
    std::span<const byte> data
)
{
    crc_v1_type crc;
    crc.process_bytes(
        data.data(),
        data.size()
    );
    return crc.checksum();
}

uint64_t hash_v1(
    std::span<const byte> data
)
{
    hash_v1_type crc;
    crc.process_bytes(
        data.data(),
        data.size()
    );
    return crc.checksum();
}

}