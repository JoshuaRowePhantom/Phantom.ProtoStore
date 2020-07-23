#include "StandardIncludes.h"
#include "Phantom.ProtoStore/src/BloomFilter.h"
#include <string>

namespace Phantom::ProtoStore
{
TEST(BloomFilterTests, get_BloomFilter_parameters)
{
    ASSERT_EQ(14378, get_BloomFilter_optimal_bit_count(.001, 1000));
    ASSERT_EQ(143776, get_BloomFilter_optimal_bit_count(.001, 10000));
    ASSERT_EQ(10, get_BloomFilter_optimal_hash_function_count_for_optimal_bit_count(.001));
    ASSERT_EQ(14, get_BloomFilter_optimal_hash_function_count_for_optimal_bit_count(.0001));
}

TEST(BloomFilterTests, prng_seeding_hash_produces_correct_number_of_values)
{
    auto hashFunction = BloomFilterHashFunctionTraits<std::hash<int>, uintmax_t>::hash_function_type(
        10
    );

    size_t bitCount = 20;
    auto hash = hashFunction.initial_hash(
        1,
        bitCount);

    { // 1
        auto extractedBitIndex = hashFunction.extract_bit_index(
            hash);
        ASSERT_GE(extractedBitIndex, 0);
        ASSERT_LE(extractedBitIndex, bitCount);
        ASSERT_TRUE(hashFunction.next_hash(hash));
    }
    { // 2
        auto extractedBitIndex = hashFunction.extract_bit_index(
            hash);
        ASSERT_GE(extractedBitIndex, 0);
        ASSERT_LE(extractedBitIndex, bitCount);
        ASSERT_TRUE(hashFunction.next_hash(hash));
    }
    { // 3
        auto extractedBitIndex = hashFunction.extract_bit_index(
            hash);
        ASSERT_GE(extractedBitIndex, 0);
        ASSERT_LE(extractedBitIndex, bitCount);
        ASSERT_TRUE(hashFunction.next_hash(hash));
    }
    { // 4
        auto extractedBitIndex = hashFunction.extract_bit_index(
            hash);
        ASSERT_GE(extractedBitIndex, 0);
        ASSERT_LE(extractedBitIndex, bitCount);
        ASSERT_TRUE(hashFunction.next_hash(hash));
    }
    { // 5
        auto extractedBitIndex = hashFunction.extract_bit_index(
            hash);
        ASSERT_GE(extractedBitIndex, 0);
        ASSERT_LE(extractedBitIndex, bitCount);
        ASSERT_TRUE(hashFunction.next_hash(hash));
    }
    { // 6
        auto extractedBitIndex = hashFunction.extract_bit_index(
            hash);
        ASSERT_GE(extractedBitIndex, 0);
        ASSERT_LE(extractedBitIndex, bitCount);
        ASSERT_TRUE(hashFunction.next_hash(hash));
    }
    { // 7
        auto extractedBitIndex = hashFunction.extract_bit_index(
            hash);
        ASSERT_GE(extractedBitIndex, 0);
        ASSERT_LE(extractedBitIndex, bitCount);
        ASSERT_TRUE(hashFunction.next_hash(hash));
    }
    { // 8
        auto extractedBitIndex = hashFunction.extract_bit_index(
            hash);
        ASSERT_GE(extractedBitIndex, 0);
        ASSERT_LE(extractedBitIndex, bitCount);
        ASSERT_TRUE(hashFunction.next_hash(hash));
    }
    { // 9
        auto extractedBitIndex = hashFunction.extract_bit_index(
            hash);
        ASSERT_GE(extractedBitIndex, 0);
        ASSERT_LE(extractedBitIndex, bitCount);
        ASSERT_TRUE(hashFunction.next_hash(hash));
    }
    { // 10
        auto extractedBitIndex = hashFunction.extract_bit_index(
            hash);
        ASSERT_GE(extractedBitIndex, 0);
        ASSERT_LE(extractedBitIndex, bitCount);
        ASSERT_FALSE(hashFunction.next_hash(hash));
    }

}

TEST(BloomFilterTests, can_construct)
{
    BloomFilter<std::hash<int>> bloomFilter(
        1024,
        10);
}

TEST(BloomFilterTests, can_add_and_test)
{
    BloomFilter<std::hash<int>> bloomFilter(
        1024,
        10);

    bloomFilter.add(1);
    ASSERT_EQ(true, bloomFilter.test(1));
}

TEST(BloomFilterTests, can_add_many_strings_with_desired_probability)
{
    auto desiredFalsePositiveErrorRate = .01;
    auto elementCount = 10000;
    auto maximumTolerableErrors = elementCount * desiredFalsePositiveErrorRate * 2;
    auto minimumTolerableErrors = elementCount * desiredFalsePositiveErrorRate / 3;

    BloomFilter<std::hash<std::string>> bloomFilter(
        get_BloomFilter_optimal_bit_count(
            desiredFalsePositiveErrorRate,
            elementCount),
        get_BloomFilter_optimal_hash_function_count_for_optimal_bit_count(
            desiredFalsePositiveErrorRate));

    std::ranlux48 rng;

    auto elementStrings = MakeRandomStrings(
        rng,
        20,
        elementCount
    );

    auto nonElementStrings = MakeRandomStrings(
        rng,
        20,
        elementCount);

    for (auto elementString : elementStrings)
    {
        bloomFilter.add(
            elementString);
    }

    for (auto elementString : elementStrings)
    {
        ASSERT_EQ(true, bloomFilter.test(
            elementString));
    }

    size_t errorCount = 0;
    for (auto nonElementString : nonElementStrings)
    {
        if (bloomFilter.test(
            nonElementString))
        {
            ++errorCount;
        }
    }

    ASSERT_GE(errorCount, minimumTolerableErrors);
    ASSERT_LE(errorCount, maximumTolerableErrors);
}


TEST(BloomFilterTests, can_save_and_restore_from_span)
{
    auto desiredFalsePositiveErrorRate = .1;
    auto elementCount = 100;
    auto maximumTolerableErrors = elementCount * desiredFalsePositiveErrorRate * 2;
    auto minimumTolerableErrors = elementCount * desiredFalsePositiveErrorRate / 3;

    BloomFilter<std::hash<std::string>> bloomFilter(
        get_BloomFilter_optimal_bit_count(
            desiredFalsePositiveErrorRate,
            elementCount),
        get_BloomFilter_optimal_hash_function_count_for_optimal_bit_count(
            desiredFalsePositiveErrorRate));

    std::ranlux48 rng;

    auto elementStrings = MakeRandomStrings(
        rng,
        20,
        elementCount
    );

    auto nonElementStrings = MakeRandomStrings(
        rng,
        20,
        elementCount);

    for (auto elementString : elementStrings)
    {
        bloomFilter.add(
            elementString);
    }

    auto span = bloomFilter.to_span();

    auto constBloomFilter = BloomFilter<std::hash<std::string>, uintmax_t, std::span<const uintmax_t>>(
        span,
        get_BloomFilter_optimal_hash_function_count_for_optimal_bit_count(
            desiredFalsePositiveErrorRate));

    for (auto elementString : elementStrings)
    {
        ASSERT_EQ(true, constBloomFilter.test(
            elementString));
    }

    size_t errorCount = 0;
    for (auto nonElementString : nonElementStrings)
    {
        if (constBloomFilter.test(
            nonElementString))
        {
            ++errorCount;
        }
    }

    ASSERT_GE(errorCount, minimumTolerableErrors);
    ASSERT_LE(errorCount, maximumTolerableErrors);
}
}