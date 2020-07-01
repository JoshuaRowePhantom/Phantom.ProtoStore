#include "Phantom.ProtoStore/src/Checksum.h"
#include "Phantom.System/utility.h"
#include <gtest/gtest.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <vector>

namespace Phantom::ProtoStore
{
    int32_t GetComputedCrc32c(
        const pooled_ptr<IChecksumAlgorithm>& checksum)
    {
        uint32_t computed;
        auto computedSpan = as_bytes(computed);

        std::copy(
            checksum->Computed().begin(),
            checksum->Computed().end(),
            computedSpan.begin());

        return computed;
    }

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

            int32_t computed = GetComputedCrc32c(
                checksum);

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

    TEST(ChecksummingZeroCopyInputStreamTests, Crc32_can_move_forward_and_backward)
    {
        auto checksumFactory = MakeChecksumAlgorithmFactory();
        auto checksum = checksumFactory->Create(
            ChecksumAlgorithmVersion::Crc32c);

        using namespace google::protobuf::io;
        std::string string1 = "1234";
        std::string string2 = "56789abc";

        ArrayInputStream stream1(
            string1.data(),
            string1.length());

        ArrayInputStream stream2(
            string2.data(),
            string2.length());

        std::vector<ZeroCopyInputStream*> inputStreams =
        {
            &stream1,
            &stream2,
        };

        ConcatenatingInputStream concatenatingInputStream(
            inputStreams.data(),
            inputStreams.size());

        {
            ChecksummingZeroCopyInputStream checksummingInputStream(
                &concatenatingInputStream,
                checksum.get());

            const void* data;
            int size;
            ASSERT_TRUE(checksummingInputStream.Next(&data, &size));
            ASSERT_EQ(4, size);
            ASSERT_EQ(4, checksummingInputStream.ByteCount());
            checksummingInputStream.BackUp(1);
            ASSERT_EQ(3, checksummingInputStream.ByteCount());
            ASSERT_TRUE(checksummingInputStream.Next(&data, &size));
            ASSERT_EQ(1, size);
            ASSERT_EQ(4, checksummingInputStream.ByteCount());
            ASSERT_TRUE(checksummingInputStream.Next(&data, &size));
            ASSERT_EQ(8, size);
            ASSERT_EQ(12, checksummingInputStream.ByteCount());
            checksummingInputStream.BackUp(3);
            ASSERT_EQ(9, checksummingInputStream.ByteCount());
        }
        checksum->Finalize();

        int32_t computed = GetComputedCrc32c(
            checksum);

        ASSERT_EQ(0xe3'06'92'83, computed);
    }

    TEST(ChecksummingZeroCopyInputStreamTests, Crc32_can_move_forward)
    {
        auto checksumFactory = MakeChecksumAlgorithmFactory();
        auto checksum = checksumFactory->Create(
            ChecksumAlgorithmVersion::Crc32c);

        using namespace google::protobuf::io;
        std::string string1 = "1234";
        std::string string2 = "56789";

        ArrayInputStream stream1(
            string1.data(),
            string1.length());

        ArrayInputStream stream2(
            string2.data(),
            string2.length());

        std::vector<ZeroCopyInputStream*> inputStreams =
        {
            &stream1,
            &stream2,
        };

        ConcatenatingInputStream concatenatingInputStream(
            inputStreams.data(),
            inputStreams.size());

        {
            ChecksummingZeroCopyInputStream checksummingInputStream(
                &concatenatingInputStream,
                checksum.get());

            const void* data;
            int size;
            ASSERT_TRUE(checksummingInputStream.Next(&data, &size));
            ASSERT_EQ(4, size);
            ASSERT_EQ(4, checksummingInputStream.ByteCount());
            ASSERT_TRUE(checksummingInputStream.Next(&data, &size));
            ASSERT_EQ(5, size);
            ASSERT_EQ(9, checksummingInputStream.ByteCount());
            ASSERT_FALSE(checksummingInputStream.Next(&data, &size));
            ASSERT_EQ(9, checksummingInputStream.ByteCount());
        }
        checksum->Finalize();

        int32_t computed = GetComputedCrc32c(
            checksum);

        ASSERT_EQ(0xe3'06'92'83, computed);
    }

    TEST(ChecksummingZeroCopyInputStreamTests, Crc32_can_skip_forward_past_eof)
    {
        auto checksumFactory = MakeChecksumAlgorithmFactory();
        auto checksum = checksumFactory->Create(
            ChecksumAlgorithmVersion::Crc32c);

        using namespace google::protobuf::io;
        std::string string1 = "1234";
        std::string string2 = "56789";

        ArrayInputStream stream1(
            string1.data(),
            string1.length());

        ArrayInputStream stream2(
            string2.data(),
            string2.length());

        std::vector<ZeroCopyInputStream*> inputStreams =
        {
            &stream1,
            &stream2,
        };

        ConcatenatingInputStream concatenatingInputStream(
            inputStreams.data(),
            inputStreams.size());

        {
            ChecksummingZeroCopyInputStream checksummingInputStream(
                &concatenatingInputStream,
                checksum.get());

            const void* data;
            int size;
            ASSERT_TRUE(checksummingInputStream.Next(&data, &size));
            ASSERT_EQ(4, size);
            ASSERT_EQ(4, checksummingInputStream.ByteCount());
            ASSERT_FALSE(checksummingInputStream.Skip(10));
            ASSERT_EQ(9, checksummingInputStream.ByteCount());
        }
        checksum->Finalize();

        int32_t computed = GetComputedCrc32c(
            checksum);

        ASSERT_EQ(0xe3'06'92'83, computed);
    }

    TEST(ChecksummingZeroCopyInputStreamTests, Crc32_can_skip_forward_before_eof)
    {
        auto checksumFactory = MakeChecksumAlgorithmFactory();
        auto checksum = checksumFactory->Create(
            ChecksumAlgorithmVersion::Crc32c);

        using namespace google::protobuf::io;
        std::string string1 = "1234";
        std::string string2 = "56789abcd";

        ArrayInputStream stream1(
            string1.data(),
            string1.length());

        ArrayInputStream stream2(
            string2.data(),
            string2.length());

        std::vector<ZeroCopyInputStream*> inputStreams =
        {
            &stream1,
            &stream2,
        };

        ConcatenatingInputStream concatenatingInputStream(
            inputStreams.data(),
            inputStreams.size());

        {
            ChecksummingZeroCopyInputStream checksummingInputStream(
                &concatenatingInputStream,
                checksum.get());

            const void* data;
            int size;
            ASSERT_TRUE(checksummingInputStream.Next(&data, &size));
            ASSERT_EQ(4, size);
            ASSERT_EQ(4, checksummingInputStream.ByteCount());
            ASSERT_TRUE(checksummingInputStream.Skip(5));
            ASSERT_EQ(9, checksummingInputStream.ByteCount());
        }
        checksum->Finalize();

        int32_t computed = GetComputedCrc32c(
            checksum);

        ASSERT_EQ(0xe3'06'92'83, computed);
    }


    TEST(ChecksummingZeroCopyInputStreamTests, Crc32_can_skip_entire_blocks)
    {
        auto checksumFactory = MakeChecksumAlgorithmFactory();
        auto checksum = checksumFactory->Create(
            ChecksumAlgorithmVersion::Crc32c);

        using namespace google::protobuf::io;
        std::string string1 = "1234";
        std::string string2 = "567";
        std::string string3 = "89abcd";

        ArrayInputStream stream1(
            string1.data(),
            string1.length());

        ArrayInputStream stream2(
            string2.data(),
            string2.length());

        ArrayInputStream stream3(
            string3.data(),
            string3.length());

        std::vector<ZeroCopyInputStream*> inputStreams =
        {
            &stream1,
            &stream2,
            &stream3,
        };

        ConcatenatingInputStream concatenatingInputStream(
            inputStreams.data(),
            inputStreams.size());

        {
            ChecksummingZeroCopyInputStream checksummingInputStream(
                &concatenatingInputStream,
                checksum.get());

            const void* data;
            int size;
            ASSERT_TRUE(checksummingInputStream.Next(&data, &size));
            ASSERT_EQ(4, size);
            ASSERT_EQ(4, checksummingInputStream.ByteCount());
            ASSERT_TRUE(checksummingInputStream.Skip(5));
            ASSERT_EQ(9, checksummingInputStream.ByteCount());
        }
        checksum->Finalize();

        int32_t computed = GetComputedCrc32c(
            checksum);

        ASSERT_EQ(0xe3'06'92'83, computed);
    }

    TEST(ChecksummingZeroCopyOutputStreamTests, Crc32_can_compute_zero_from_empty_buffer)
    {
        auto checksumFactory = MakeChecksumAlgorithmFactory();
        auto checksum = checksumFactory->Create(
            ChecksumAlgorithmVersion::Crc32c);
        using namespace google::protobuf::io;

        {
            ArrayOutputStream arrayOutputStream(nullptr, 0);
            ChecksummingZeroCopyOutputStream outputStream(
                &arrayOutputStream,
                checksum.get());

            void* data;
            int size;
            ASSERT_FALSE(outputStream.Next(&data, &size));
        }
        checksum->Finalize();

        int32_t computed = GetComputedCrc32c(
            checksum);

        ASSERT_EQ(0, computed);
    }

    TEST(ChecksummingZeroCopyOutputStreamTests, Crc32_can_compute_zero_from_non_empty_buffer_with_backup)
    {
        auto checksumFactory = MakeChecksumAlgorithmFactory();
        auto checksum = checksumFactory->Create(
            ChecksumAlgorithmVersion::Crc32c);
        using namespace google::protobuf::io;

        {
            std::string stringData;
            StringOutputStream stringOutputStream(&stringData);
            ChecksummingZeroCopyOutputStream outputStream(
                &stringOutputStream,
                checksum.get());

            void* data;
            int size;
            ASSERT_TRUE(outputStream.Next(&data, &size));
            ASSERT_GT(size, 0);
            outputStream.BackUp(size);
        }
        checksum->Finalize();

        int32_t computed = GetComputedCrc32c(
            checksum);

        ASSERT_EQ(0, computed);
    }

    TEST(ChecksummingZeroCopyOutputStreamTests, Crc32_can_compute_nonzero_from_BackUp)
    {
        auto checksumFactory = MakeChecksumAlgorithmFactory();
        auto checksum = checksumFactory->Create(
            ChecksumAlgorithmVersion::Crc32c);
        using namespace google::protobuf::io;

        {
            std::string stringData;
            stringData.reserve(100);
            StringOutputStream stringOutputStream(&stringData);
            ChecksummingZeroCopyOutputStream outputStream(
                &stringOutputStream,
                checksum.get());

            void* data;
            int size;
            ASSERT_TRUE(outputStream.Next(&data, &size));
            ASSERT_GT(size, 0);
            memcpy_s(data, size, "123456789abcdefg", 16);
            outputStream.BackUp(size - 9);
        }
        checksum->Finalize();

        int32_t computed = GetComputedCrc32c(
            checksum);

        ASSERT_EQ(0xe3'06'92'83, computed);
    }

    TEST(ChecksummingZeroCopyOutputStreamTests, Crc32_can_compute_nonzero_without_BackUp)
    {
        auto checksumFactory = MakeChecksumAlgorithmFactory();
        auto checksum = checksumFactory->Create(
            ChecksumAlgorithmVersion::Crc32c);
        using namespace google::protobuf::io;

        {
            std::string stringData;
            stringData.resize(32);
            ArrayOutputStream arrayOutputStream(stringData.data(), stringData.size());
            ChecksummingZeroCopyOutputStream outputStream(
                &arrayOutputStream,
                checksum.get());

            void* data;
            int size;
            ASSERT_TRUE(outputStream.Next(&data, &size));
            ASSERT_EQ(size, 32);
            memset(data, 0, size);
        }
        checksum->Finalize();

        int32_t computed = GetComputedCrc32c(
            checksum);

        ASSERT_EQ(0x8a'91'36'aa, computed);
    }

    TEST(ChecksummingZeroCopyOutputStreamTests, Actually_copies_the_input_to_the_output)
    {
        auto checksumFactory = MakeChecksumAlgorithmFactory();
        auto checksum = checksumFactory->Create(
            ChecksumAlgorithmVersion::Crc32c);
        using namespace google::protobuf::io;

        std::string stringData;
        {
            stringData.resize(13);
            ArrayOutputStream arrayOutputStream(stringData.data(), stringData.size(), 5);
            ChecksummingZeroCopyOutputStream outputStream(
                &arrayOutputStream,
                checksum.get());

            void* data;
            int size;
            ASSERT_TRUE(outputStream.Next(&data, &size));
            ASSERT_EQ(size, 5);
            ASSERT_EQ(5, outputStream.ByteCount());
            memcpy_s(data, size, "1___1", 5);

            ASSERT_TRUE(outputStream.Next(&data, &size));
            ASSERT_EQ(size, 5);
            ASSERT_EQ(10, outputStream.ByteCount());
            memcpy_s(data, size, "2___2", 5);

            ASSERT_TRUE(outputStream.Next(&data, &size));
            ASSERT_EQ(size, 3);
            ASSERT_EQ(13, outputStream.ByteCount());
            memcpy_s(data, size, "3", 3);

            outputStream.BackUp(2);
            ASSERT_EQ(11, outputStream.ByteCount());
        }
        checksum->Finalize();

        using namespace std::string_literals;

        std::string expectedString = "1___12___23\0\0"s;

        ASSERT_EQ(expectedString, stringData);
    }

}