#include "StandardTypes.h"
#include "ValueComparer.h"
#include "Phantom.ProtoStore/ProtoStore.pb.h"
#include "ProtoStoreInternal.pb.h"
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/wire_format_lite.h>
#include <compare>
#include <set>
#include "Checksum.h"

namespace Phantom::ProtoStore
{

using namespace google::protobuf;

std::weak_ordering BaseValueComparer::ApplySortOrder(
    SortOrder sortOrder,
    std::weak_ordering value
)
{
    if (sortOrder == SortOrder::Ascending)
    {
        return value;
    }

    return 0 <=> value;
}

SortOrder BaseValueComparer::CombineSortOrder(
    SortOrder sortOrder1,
    SortOrder sortOrder2
)
{
    if (sortOrder2 == SortOrder::Ascending)
    {
        return sortOrder1;
    }
    if (sortOrder1 == SortOrder::Ascending)
    {
        return SortOrder::Descending;
    }
    return SortOrder::Ascending;
}

std::weak_ordering KeyRangeComparer::operator()(
    const KeyAndSequenceNumberComparerArgument& left,
    const KeyRangeComparerArgument& right
    ) const
{
    auto result = m_keyComparer.Compare(
        left.Key,
        right.Key);

    if (result == std::weak_ordering::equivalent)
    {
        if (right.Inclusivity == Inclusivity::Exclusive)
        {
            result = std::weak_ordering::less;
        }
    }

    // Intentionally backward, so that later sequence numbers compare earlier.
    if (result == std::weak_ordering::equivalent)
    {
        result = right.SequenceNumber <=> left.SequenceNumber;
    }

    return result;
}

std::weak_ordering KeyRangeComparer::operator()(
    const KeyRangeComparerArgument& left,
    const KeyAndSequenceNumberComparerArgument& right
    ) const
{
    auto result = m_keyComparer.Compare(
        left.Key,
        right.Key);

    if (result == std::weak_ordering::equivalent)
    {
        if (left.Inclusivity == Inclusivity::Exclusive)
        {
            result = std::weak_ordering::greater;
        }
    }

    // Intentionally backward, so that later sequence numbers compare earlier.
    if (result == std::weak_ordering::equivalent)
    {
        result = right.SequenceNumber <=> left.SequenceNumber;
    }

    return result;
}

std::weak_ordering ValueComparer::Compare(
    const ProtoValue& value1,
    const ProtoValue& value2
) const
{
    if (value1.IsKeyMin() && value2.IsKeyMin())
    {
        return std::weak_ordering::equivalent;
    }
    if (value1.IsKeyMax() && value2.IsKeyMax())
    {
        return std::weak_ordering::equivalent;
    }
    if (value1.IsKeyMin())
    {
        return std::weak_ordering::less;
    }
    if (value1.IsKeyMax())
    {
        return std::weak_ordering::greater;
    }
    if (value2.IsKeyMin())
    {
        return std::weak_ordering::greater;
    }
    if (value2.IsKeyMax())
    {
        return std::weak_ordering::less;
    }

    return CompareImpl(value1, value2);
}

std::weak_ordering ValueComparer::operator()(
    const ProtoValue& value1,
    const ProtoValue& value2
    ) const
{
    return Compare(value1, value2);
}


}
