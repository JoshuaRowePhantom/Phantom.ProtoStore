#include "Phantom.ProtoStore/src/Checksum.h"
#include "Phantom.System/utility.h"
#include <gtest/gtest.h>
#include <vector>

namespace Phantom::ProtoStore
{
    bool RunCrc32c(
        const std::string& bytes,
        uint32_t expectedResult
    )
    {
        auto result = checksum_v1(
            as_bytes(std::span(bytes))
        );

        EXPECT_EQ(expectedResult, result);

        return true;
    }

    TEST(ChecksumTests, Crc32TestVectors_0)
    {
        EXPECT_TRUE(RunCrc32c({ }, 0));
    }
    
    TEST(ChecksumTests, Crc32TestVectors_a)
    {
        EXPECT_TRUE(RunCrc32c({ "a" }, 0xc1'd0'43'30));
    }
    
    TEST(ChecksumTests, Crc32TestVectors_123456789)
    {
        EXPECT_TRUE(RunCrc32c({ "123456789" }, 0xe3'06'92'83));
    }

}