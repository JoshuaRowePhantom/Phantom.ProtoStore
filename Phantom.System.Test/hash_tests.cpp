#include "StandardIncludes.h"
#include "Phantom.System/hash.h"

namespace Phantom
{
TEST(hash_tests, hash_string_is_same_as_std_hash_string)
{
    std::hash<std::string> std_hash;
    ASSERT_EQ(std_hash("hello world"), hash(std::string("hello world")));
}

TEST(hash_tests, hash_two_strings_are_combined)
{
    std::hash<std::string> std_hash;

    auto hash1 = std_hash(std::string("hello"));
    auto hash2 = std_hash(std::string("world"));
    auto combined_hash2 = hash1 ^ (
        hash2 + 0x9ddfea08eb382d69ULL + (hash1 << 17) + (hash1 >> 47));

    ASSERT_EQ(combined_hash2, (hash(std::string("hello"), std::string("world"))));
}

TEST(hash_tests, hash_three_strings_are_combined)
{
    std::hash<std::string> std_hash;
    
    auto hash1 = std_hash(std::string("hello"));
    auto hash2 = std_hash(std::string("world"));
    auto hash3 = std_hash(std::string("blammo"));
    auto combined_hash2 = hash1 ^ (
        hash2 + 0x9ddfea08eb382d69ULL + (hash1 << 17) + (hash1 >> 47));
    auto combined_hash3 = combined_hash2 ^ (
        hash3 + 0x9ddfea08eb382d69ULL + (combined_hash2 << 17) + (combined_hash2 >> 47));

    ASSERT_EQ(combined_hash3, (hash(std::string("hello"), std::string("world"), std::string("blammo"))));
}

}