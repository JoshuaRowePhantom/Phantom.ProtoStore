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

std::weak_ordering KeyRangeComparer::lower_bound_compare(
    const ProtoValue& left,
    const KeyRangeComparerArgument& right
    ) const
{
    if (right.LastFieldId
        && m_keyComparer.IsPrefixOf(
            Prefix
            {
                right.Key,
                *right.LastFieldId
            },
            left))
    {
        return std::weak_ordering::greater;
    }

    auto result = m_keyComparer.Compare(
        left,
        right.Key);

    if (result == std::weak_ordering::equivalent)
    {
        if (right.Inclusivity == Inclusivity::Exclusive)
        {
            result = std::weak_ordering::less;
        }
        else
        {
            // The KeyRangeComparer has to look for boundaries between keys, but doesn't pay attention to
            // sequence numbers.
            // Because there might be an equivalent key with a higher sequence number, the KeyRangeComparer
            // never returns equivalent.
            result = std::weak_ordering::greater;
        }
    }

    return result;
}

std::weak_ordering KeyRangeComparer::upper_bound_compare(
    const KeyRangeComparerArgument& left,
    const ProtoValue& right
    ) const
{
    if (left.LastFieldId
        && m_keyComparer.IsPrefixOf(
            Prefix
            {
                left.Key,
                *left .LastFieldId
            },
            right))
    {
        return std::weak_ordering::greater;
    }

    auto result = m_keyComparer.Compare(
        left.Key,
        right);

    if (result == std::weak_ordering::equivalent)
    {
        if (left.Inclusivity == Inclusivity::Exclusive)
        {
            result = std::weak_ordering::less;
        }
        else
        {
            // The KeyRangeComparer has to look for boundaries between keys, but doesn't pay attention to
            // sequence numbers.
            // Because there might be an equivalent key with a lower sequence number, the KeyRangeComparer
            // never returns equivalent.
            result = std::weak_ordering::greater;
        }
    }

    return result;
}

bool KeyRangeComparer::lower_bound_less(
    const ProtoValue& left,
    const KeyRangeComparerArgument& right
) const
{
    return lower_bound_compare(left, right) == std::weak_ordering::less;
}

bool KeyRangeComparer::upper_bound_less(
    const KeyRangeComparerArgument& left,
    const ProtoValue& right
) const
{
    return upper_bound_compare(left, right) == std::weak_ordering::less;
}

bool ValueComparer::EqualsImpl(
    const ProtoValue& value1,
    const ProtoValue& value2
) const
{
    return std::weak_ordering::equivalent == Compare(
        value1,
        value2);
}

std::optional<bool> ValueComparer::EqualsKeyMinMax(
    const ProtoValue& value1,
    const ProtoValue& value2
)
{
    if (value1.IsKeyMin() || value1.IsKeyMax() || value2.IsKeyMin() || value2.IsKeyMax())
    {
        return value1.IsKeyMin() && value2.IsKeyMin()
            || value1.IsKeyMax() && value2.IsKeyMax();
    }
    return std::nullopt;
}

bool ValueComparer::Equals(
    const ProtoValue& value1,
    const ProtoValue& value2
) const
{
    auto keyMinMaxEqualsResult = EqualsKeyMinMax(value1, value2);
    if (keyMinMaxEqualsResult)
    {
        return *keyMinMaxEqualsResult;
    }

    return EqualsImpl(value1, value2);
}

std::weak_ordering ValueComparer::to_weak_ordering(
    std::partial_ordering ordering
)
{
    assert(ordering == std::partial_ordering::less
        || ordering == std::partial_ordering::greater
        || ordering == std::partial_ordering::equivalent);

    if (ordering == std::partial_ordering::less)
    {
        return std::weak_ordering::less;
    }
    if (ordering == std::partial_ordering::greater)
    {
        return std::weak_ordering::greater;
    }
    if (ordering == std::weak_ordering::equivalent)
    {
        return std::weak_ordering::equivalent;
    }
    std::unreachable();
}

std::partial_ordering ValueComparer::CompareKeyMinMax(
    const ProtoValue& value1,
    const ProtoValue& value2
)
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

    return std::partial_ordering::unordered;
}

std::weak_ordering ValueComparer::Compare(
    const ProtoValue& value1,
    const ProtoValue& value2
) const
{
    auto keyMinMaxComparison = CompareKeyMinMax(
        value1,
        value2);
    if (keyMinMaxComparison != std::partial_ordering::unordered)
    {
        return to_weak_ordering(keyMinMaxComparison);
    }

    return CompareImpl(value1, value2);
}


}
