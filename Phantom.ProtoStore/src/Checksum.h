#pragma once

#include "StandardTypes.h"
#include <google/protobuf/io/zero_copy_stream.h>

namespace Phantom::ProtoStore
{
    enum class ChecksumAlgorithmVersion : uint8_t
    {
        Default = 0,
        Crc32c = 1,
    };

    class IChecksumAlgorithm
    {
    public:
        virtual ChecksumAlgorithmVersion Version() const = 0;
        virtual size_t SizeInBytes() const = 0;

        virtual void AddData(
            span<const byte> data) = 0;

        virtual void Finalize() = 0;
        virtual span<byte> Comparand() = 0;
        virtual span<const byte> Computed() const = 0;
        virtual bool IsValid() const = 0;

        virtual void ReturnToPool() = 0;
    };

    class IChecksumAlgorithmFactory
    {
    public:
        virtual pooled_ptr<IChecksumAlgorithm> Create(
            ChecksumAlgorithmVersion version = ChecksumAlgorithmVersion::Default) const = 0;
    };

    class ChecksummingZeroCopyInputStream
        :
        public ZeroCopyInputStream
    {
        IChecksumAlgorithm* m_checksum;
        ZeroCopyInputStream* m_baseStream;
        
        size_t m_offset;
        size_t m_checksummedOffset;
        const void* m_lastData;
        int m_lastSize;

        void AddLastData();

    public:
        ChecksummingZeroCopyInputStream(
            ZeroCopyInputStream* baseStream,
            IChecksumAlgorithm* checksum);

        ~ChecksummingZeroCopyInputStream();

        virtual bool Next(
            const void** data,
            int* size
        ) override;

        virtual void BackUp(
            int count
        ) override;

        virtual bool Skip(
            int count
        ) override;

        virtual int64_t ByteCount() const override;
    };

    class ChecksummingZeroCopyOutputStream
        :
        public ZeroCopyOutputStream
    {
        IChecksumAlgorithm* m_checksum;
        ZeroCopyOutputStream* m_baseStream;

        size_t m_offset;
        size_t m_checksummedOffset;
        void* m_lastData;
        int m_lastSize;

        void AddLastData();

    public:
        ChecksummingZeroCopyOutputStream(
            ZeroCopyOutputStream* baseStream,
            IChecksumAlgorithm* checksum);

        ~ChecksummingZeroCopyOutputStream();

        virtual bool Next(
            void** data,
            int* size
        ) override;

        virtual void BackUp(
            int count
        ) override;

        virtual int64_t ByteCount() const override;
    };

    shared_ptr<IChecksumAlgorithmFactory> MakeChecksumAlgorithmFactory();
}
