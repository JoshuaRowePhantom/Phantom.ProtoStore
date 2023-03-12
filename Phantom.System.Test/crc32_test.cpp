#include "StandardIncludes.h"
#include "Phantom.System/crc32.h"

namespace Phantom
{

TEST(crc32_test, test_vector_empty)
{
    crc32<0xffffffff, 0> crc;
    EXPECT_EQ(4294967295, crc.checksum());
    EXPECT_EQ(4294967295, crc());
}

TEST(crc32_test, test_vector_a)
{
    crc32< 0xffffffff, 0> crc;
    crc.process_byte('a');
    EXPECT_EQ(1043315919UL, crc.checksum());
    EXPECT_EQ(1043315919UL, crc());
}

TEST(crc32_test, test_vector_ab_individual_bytes)
{
    crc32<0xffffffff, 0> crc;
    crc.process_byte('a');
    crc.process_byte('b');
    EXPECT_EQ(492689097UL, crc.checksum());
    EXPECT_EQ(492689097UL, crc());
}

TEST(crc32_test, test_vector_ab_byte_array)
{
    crc32<0xffffffff, 0> crc;
    crc.process_bytes("ab", 2);
    EXPECT_EQ(492689097UL, crc.checksum());
    EXPECT_EQ(492689097UL, crc());
}

TEST(crc32_test, test_vector_ab_byte_array_offset)
{
    crc32<0xffffffff, 0> crc;
    std::string value = "xab";
    crc.process_bytes(value.data() + 1, 2);
    EXPECT_EQ(492689097UL, crc.checksum());
    EXPECT_EQ(492689097UL, crc());
}

TEST(crc32_test, test_vector_123456789)
{
    crc32<0xffffffff, 0> crc;
    std::string_view buffer = "123456789";
    crc.process_bytes(buffer.data(), buffer.size());
    EXPECT_EQ(486108540UL, crc.checksum());
    EXPECT_EQ(486108540UL, crc());
}

TEST(buffered_crc_test, test_vector_123456789)
{
    buffered_crc<256, crc32<0xffffffff, 0>> crc;
    std::string_view buffer = "123456789";
    crc.process_bytes(buffer.data(), buffer.size());
    EXPECT_EQ(486108540UL, crc.checksum());
    EXPECT_EQ(486108540UL, crc());
}

TEST(buffered_crc_test, large_value)
{
    crc32<0xffffffff, 0> crc;
    buffered_crc<64, crc32<0xffffffff, 0>> buffered_crc;
    std::string s1 = "123456789";

    for (auto counter = 0; counter < 100; ++counter)
    {
        crc.process_bytes(s1.data(), s1.size());
        buffered_crc.process_bytes(s1.data(), s1.size());
    }

    EXPECT_EQ(crc.checksum(), buffered_crc.checksum());
}

}
