#include "Checksum.h"
#include <boost/crc.hpp>

namespace Phantom::ProtoStore
{

uint32_t checksum_v1(
    std::span<const byte> data
)
{
    using crc_v1_type = boost::crc_optimal<32, 0x1EDC6F41, 0xffffffff, 0, true, true>;
    crc_v1_type crc;
    crc.process_bytes(
        data.data(),
        data.size()
    );
    return crc.checksum();
}

}