#pragma once

#include <intrin.h>
#include<array>

namespace Phantom
{

template<
    uint32_t InitRemainder,
    uint32_t FinalXor
>
class crc32
{
    static constexpr uint32_t HardwareFinalXor = 0xffffffff;

public:
    using value_type = uint32_t;

    void process_byte(unsigned char byte)
    {
        m_remainder = _mm_crc32_u8(m_remainder, byte);
    }

    void process_block(void const* bytes_begin, void const* bytes_end)
    {
        process_bytes(
            reinterpret_cast<const char*>(bytes_begin),
            reinterpret_cast<const char*>(bytes_end) - reinterpret_cast<const char*>(bytes_begin)
        );
    }

    void process_bytes(void const* buffer, std::size_t byte_count)
    {
        internal_process_bytes(
            reinterpret_cast<const char*>(buffer),
            byte_count);
    }

    value_type checksum() const
    {
        return m_remainder ^ FinalXor;
    }

    // Operators
    void operator ()(unsigned char byte)
    {
        return process_byte(byte);
    }

    value_type operator ()() const
    {
        return checksum();
    }

private:
    value_type m_remainder = InitRemainder;

    void internal_process_aligned_bytes(
        const unsigned char* bufferStart,
        const unsigned char* bufferEnd
    )
    {
        uintptr_t bufferInt = reinterpret_cast<uintptr_t>(bufferStart);
        uintptr_t bufferEndInt = reinterpret_cast<uintptr_t>(bufferEnd);

        // Process 8 bytes at a time until near the end of the buffer.
        while ((bufferInt + 8) < bufferEndInt)
        {
            m_remainder = _mm_crc32_u64(
                m_remainder,
                *reinterpret_cast<const uint64_t*>(bufferInt));
            bufferInt += 8;
        }

        // Process individual bytes to the end of the buffer.
        while (bufferInt < bufferEndInt)
        {
            process_byte(
                *reinterpret_cast<const char*>(bufferInt)
            );
            ++bufferInt;
        }
    }

    void internal_process_bytes(
        const char* buffer,
        std::size_t byte_count)
    {
        uintptr_t bufferInt = reinterpret_cast<uintptr_t>(buffer);
        uintptr_t bufferEndInt = reinterpret_cast<uintptr_t>(buffer + byte_count);

        // Process individual bytes to the next 8-byte alignment.
        while (
            /* (bufferInt & 0x7) != 0
            && */ bufferInt < bufferEndInt)
        {
            process_byte(
                *reinterpret_cast<const char*>(bufferInt)
            );
            ++bufferInt;
        }

        internal_process_aligned_bytes(
            reinterpret_cast<const unsigned char*>(bufferInt),
            reinterpret_cast<const unsigned char*>(bufferEndInt)
        );
    }
};

template<
    size_t BufferSize,
    typename Crc
> class buffered_crc
{
    mutable Crc m_crc;
    std::array<char, BufferSize> m_buffer;
    mutable size_t m_bufferOffset = 0;

public:
    using value_type = typename Crc::value_type;

    void process_byte(unsigned char byte)
    {
        m_buffer[m_bufferOffset] = byte;
        if (m_bufferOffset == BufferSize)
        {
            checksum();
        }
    }

    void process_block(void const* bytes_begin, void const* bytes_end)
    {
        process_bytes(
            reinterpret_cast<const char*>(bytes_begin),
            reinterpret_cast<const char*>(bytes_end) - reinterpret_cast<const char*>(bytes_begin)
        );
    }

    void process_bytes(void const* buffer, std::size_t byte_count)
    {
        if (m_bufferOffset + byte_count < BufferSize)
        {
            memcpy(m_buffer.data() + m_bufferOffset, buffer, byte_count);
            m_bufferOffset += byte_count;
        }
        else
        {
            checksum();
            m_crc.process_bytes(buffer, byte_count);
        }
    }

    value_type checksum() const
    {
        m_crc.process_bytes(m_buffer.data(), m_bufferOffset);
        m_bufferOffset = 0;
        return m_crc.checksum();
    }

    // Operators
    void operator ()(unsigned char byte)
    {
        return process_byte(byte);
    }

    value_type operator ()() const
    {
        return checksum();
    }

};
}