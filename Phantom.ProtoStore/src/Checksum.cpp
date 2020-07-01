#include "Checksum.h"
#include <boost/crc.hpp>

namespace Phantom::ProtoStore
{

    void ChecksummingZeroCopyInputStream::AddLastData()
    {
        if (!m_lastData)
        {
            return;
        }

        m_checksum->AddData(
            std::span(
                reinterpret_cast<const std::byte*>(m_lastData),
                m_lastSize)
        );

        m_lastData = nullptr;
    }

    ChecksummingZeroCopyInputStream::ChecksummingZeroCopyInputStream(
        google::protobuf::io::ZeroCopyInputStream* baseStream,
        IChecksumAlgorithm* checksum)
        :
        m_baseStream(baseStream),
        m_checksum(checksum),
        m_offset(0),
        m_checksummedOffset(0),
        m_lastData(nullptr),
        m_lastSize(0)
    {
    }

    ChecksummingZeroCopyInputStream::~ChecksummingZeroCopyInputStream()
    {
        AddLastData();
    }

    bool ChecksummingZeroCopyInputStream::Next(
        const void** data,
        int* size
    )
    {
        AddLastData();

        auto result = m_baseStream->Next(
            &m_lastData,
            &m_lastSize);

        *data = m_lastData;
        *size = m_lastSize;

        return result;
    }

    void ChecksummingZeroCopyInputStream::BackUp(
        int count
    )
    {
        m_lastSize -= count;
        AddLastData();
        m_baseStream->BackUp(count);
    }

    bool ChecksummingZeroCopyInputStream::Skip(
        int count
    ) 
    {
        auto endPosition = ByteCount() + count;
        const void* data;
        int size;
        auto result = true;

        while (ByteCount() < endPosition
            &&
            (result = Next(&data, &size)))
        {
        }

        if (ByteCount() > endPosition)
        {
            BackUp(ByteCount() - endPosition);
        }

        return result;
    }

    int64_t ChecksummingZeroCopyInputStream::ByteCount() const
    {
        return m_baseStream->ByteCount();
    }

    void ChecksummingZeroCopyOutputStream::AddLastData()
    {
        if (!m_lastData)
        {
            return;
        }

        m_checksum->AddData(
            std::span(
                reinterpret_cast<const std::byte*>(m_lastData),
                m_lastSize)
        );

        m_lastData = nullptr;
    }

    ChecksummingZeroCopyOutputStream::ChecksummingZeroCopyOutputStream(
        google::protobuf::io::ZeroCopyOutputStream* baseStream,
        IChecksumAlgorithm* checksum)
        :
        m_baseStream(baseStream),
        m_checksum(checksum),
        m_offset(0),
        m_checksummedOffset(0),
        m_lastData(nullptr),
        m_lastSize(0)
    {
    }

    ChecksummingZeroCopyOutputStream::~ChecksummingZeroCopyOutputStream()
    {
        AddLastData();
    }

    bool ChecksummingZeroCopyOutputStream::Next(
        void** data,
        int* size
    )
    {
        AddLastData();

        auto result = m_baseStream->Next(
            &m_lastData,
            &m_lastSize);

        *data = m_lastData;
        *size = m_lastSize;

        return result;
    }

    void ChecksummingZeroCopyOutputStream::BackUp(
        int count
    )
    {
        m_lastSize -= count;
        AddLastData();
        m_baseStream->BackUp(count);
    }

    int64_t ChecksummingZeroCopyOutputStream::ByteCount() const
    {
        return m_baseStream->ByteCount();
    }

    class Crc32cChecksum :
        public IChecksumAlgorithm
    {
        typedef boost::crc_optimal<32, 0x1EDC6F41, 0xffffffff, 0xffffffff, true, true> crc_type;
        uint32_t m_comparand;
        uint32_t m_computed;
        crc_type m_crc;

    public:
        Crc32cChecksum()
            :
            m_comparand(0),
            m_computed(0)
        {}

        virtual ChecksumAlgorithmVersion Version() const override
        {
            return ChecksumAlgorithmVersion::Crc32c;
        }

        virtual size_t SizeInBytes() const override
        {
            return sizeof(uint32_t);
        }

        virtual void AddData(
            std::span<const std::byte> data
        ) override
        {
            m_crc.process_bytes(
                data.data(),
                data.size_bytes());
        }

        virtual void Finalize() override
        {
            m_computed = m_crc.checksum();
        }

        virtual std::span<std::byte> Comparand() override
        {
            return as_writable_bytes(
                std::span(&m_comparand, 1)
            );
        }

        virtual std::span<const std::byte> Computed() const override
        {
            return as_bytes(
                std::span(&m_computed, 1)
            );
        }

        virtual bool IsValid() const override
        {
            return m_comparand == m_computed;
        }

        virtual void ReturnToPool() override
        {
            delete this;
        }
    };

    class ChecksumAlgorithmFactory : 
        public IChecksumAlgorithmFactory
    {
        // Inherited via IChecksumAlgorithmFactory
        virtual pooled_ptr<IChecksumAlgorithm> Create(
            ChecksumAlgorithmVersion version = ChecksumAlgorithmVersion::Default) 
            const override
        {
            switch (version)
            {
            case ChecksumAlgorithmVersion::Default:
            case ChecksumAlgorithmVersion::Crc32c:
                return pooled_ptr<IChecksumAlgorithm>(
                    new Crc32cChecksum());
            default:
                throw std::range_error("Invalid checksum algorithm");
            }
        }
    };

    std::shared_ptr<IChecksumAlgorithmFactory> MakeChecksumAlgorithmFactory()
    {
        return std::make_shared<ChecksumAlgorithmFactory>();
    }
}