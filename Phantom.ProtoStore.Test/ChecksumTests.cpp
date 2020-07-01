#include "Phantom.ProtoStore/src/Checksum.h"
#include "Phantom.System/utility.h"
#include <gtest/gtest.h>

namespace Phantom::ProtoStore
{
    TEST(ChecksumTests, DefaultChecksumIsCrc32c)
    {
        auto checksumFactory = MakeChecksumAlgorithmFactory();
        auto checksum1 = checksumFactory->Create();
        ASSERT_EQ(ChecksumAlgorithmVersion::Crc32c, checksum1->Version());
        auto checksum2 = checksumFactory->Create(
            ChecksumAlgorithmVersion::Default);
        ASSERT_EQ(ChecksumAlgorithmVersion::Crc32c, checksum2->Version());
    }

    bool RunCrc32c(
        const std::string& bytes,
        uint32_t expectedResult
    )
    {
        auto checksumFactory = MakeChecksumAlgorithmFactory();

        auto bytesData = as_bytes(std::span(
            bytes.data(),
            bytes.length()));

        for (size_t stride = 1; stride <= bytes.size() + 1; stride++)
        {
            auto checksum = checksumFactory->Create(
                ChecksumAlgorithmVersion::Crc32c);

            for (size_t start = 0; start < bytes.size(); start += stride)
            {
                checksum->AddData(
                    bytesData.subspan(
                        start,
                        std::min(stride, bytes.size() - start))
                    );
            }
            checksum->Finalize();

            int32_t computed;
            auto computedSpan = as_bytes(computed);

            std::copy(
                checksum->Computed().begin(),
                checksum->Computed().end(),
                computedSpan.begin());

            EXPECT_EQ(expectedResult, computed);

            auto invalidComparand = expectedResult + 1;
            auto comparandSpan = as_bytes(invalidComparand);

            std::copy(
                comparandSpan.begin(),
                comparandSpan.end(),
                checksum->Comparand().begin());

            EXPECT_FALSE(checksum->IsValid());

            comparandSpan = as_bytes(expectedResult);

            std::copy(
                comparandSpan.begin(),
                comparandSpan.end(),
                checksum->Comparand().begin());

            EXPECT_TRUE(checksum->IsValid());
        }

        return true;
    }

    TEST(ChecksumTests, Crc32TestVectors_0)
    {
        ASSERT_TRUE(RunCrc32c({ }, 0));
    }
    
    TEST(ChecksumTests, Crc32TestVectors_a)
    {
        ASSERT_TRUE(RunCrc32c({ "a" }, 0xc1'd0'43'30));
    }
    
    TEST(ChecksumTests, Crc32TestVectors_123456789)
    {
        ASSERT_TRUE(RunCrc32c({ "123456789" }, 0xe3'06'92'83));
    }
}