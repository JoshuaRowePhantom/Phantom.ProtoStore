#pragma once

#include <stdint.h>
#include <string>
#include <string_view>

namespace Phantom::ProtoStore
{

typedef std::string IndexName;
typedef std::string_view TransactionId;

enum class SequenceNumber : std::uint64_t
{
    Earliest = 0,
    Latest = 0x3fffffffffffffffULL,
    LatestCommitted = Latest - 1,
};

inline SequenceNumber ToSequenceNumber(
    std::uint64_t sequenceNumber)
{
    if (sequenceNumber > static_cast<uint64_t>(SequenceNumber::Latest))
    {
        throw std::out_of_range(
            "SequenceNumber was out of range.");
    }

    return static_cast<SequenceNumber>(sequenceNumber << 2);
}

inline std::uint64_t ToUint64(
    SequenceNumber sequenceNumber)
{
    return static_cast<std::uint64_t>(sequenceNumber) >> 2;
}

inline std::weak_ordering operator <=>(
    SequenceNumber s1,
    SequenceNumber s2
    )
{
    return ToUint64(s1) <=> ToUint64(s2);
}

}
