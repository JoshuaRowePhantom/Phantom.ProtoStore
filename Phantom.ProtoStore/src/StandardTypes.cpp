#include "StandardTypes.h"
#include "src/ProtoStoreInternal_generated.h"

namespace Phantom::ProtoStore
{

std::span<const byte> get_byte_span(
    const flatbuffers::Vector<int8_t>* value
)
{
    if (!value)
    {
        return {};
    }

    return std::span
    {
        reinterpret_cast<const byte*>(value->data()),
        size_t(value->size())
    };
}

std::span<const byte> get_byte_span(
    const std::string& value
)
{
    return std::span
    {
        reinterpret_cast<const byte*>(value.data()),
        value.size()
    };
}

std::span<const char> get_char_span(
    std::span<const std::byte> value
)
{
    return std::span
    {
        reinterpret_cast<const char*>(value.data()),
        value.size()
    };
}

std::span<const uint8_t> get_uint8_t_span(
    std::span<const std::byte> value
)
{
    return std::span
    {
        reinterpret_cast<const uint8_t*>(value.data()),
        value.size()
    };
}

std::span<const int8_t> get_int8_t_span(
    std::span<const std::byte> value
)
{
    return std::span
    {
        reinterpret_cast<const int8_t*>(value.data()),
        value.size()
    };
}

namespace FlatBuffers
{

template<
    typename ... MemberType,
    typename Union
>
std::weak_ordering compare_union(
    const Union& left,
    const Union& right,
    const MemberType* (Union::*...accessor)() const
)
{
    std::weak_ordering result = std::weak_ordering::equivalent;

    auto comparer = [&](auto accessor)
    {
        auto leftValue = (left.*accessor)();
        auto rightValue = (right.*accessor)();

        if (!leftValue && !rightValue)
        {
            return std::weak_ordering::equivalent;
        }
        if (!leftValue)
        {
            return std::weak_ordering::less;
        }
        if (!rightValue)
        {
            return std::weak_ordering::greater;
        }

        return *leftValue <=> *rightValue;
    };

    (
        ((result = comparer(accessor)) == std::weak_ordering::equivalent)
        &&
        ...
        );

    return result;
}

template<
    IsNativeTable Object,
    std::invocable Comparer
>
std::weak_ordering compare(
    const Object& left,
    const Object& right,
    Comparer&& comparer
)
{
    return comparer;
}

template<
    IsNativeTable Object,
    typename OtherObject
>
std::weak_ordering compare(
    const Object& left,
    const Object& right,
    OtherObject Object::* primitiveField
)
{
    return left.*primitiveField <=> right.*primitiveField;
}

template<
    IsNativeTable Object,
    IsNativeTable OtherObject
>
std::weak_ordering compare(
    const Object& left,
    const Object& right,
    OtherObject* Object::* nativeTableField
)
{
    return *left.*nativeTableField <=> *right.*nativeTableField;
}

template<
    typename Object,
    typename ... Comparers
>
std::weak_ordering compare(
    const Object& left,
    const Object& right,
    Comparers&& ... comparers
)
{
    std::weak_ordering result = std::weak_ordering::equivalent;
    auto do_comparison = [&](auto&& comparer)
    {
        result = compare(left, right, std::forward<decltype(comparer)>(comparer));
        return result == std::weak_ordering::equivalent;
    };

    (
        ...
        &&
        do_comparison(comparers)
        );

    return result;
}

std::weak_ordering operator<=>(
    const ExtentNameT& left,
    const ExtentNameT& right
    )
{
    return compare_union(
        left.extent_name,
        right.extent_name,
        &FlatBuffers::ExtentNameUnionUnion::AsDatabaseHeaderExtentName,
        &FlatBuffers::ExtentNameUnionUnion::AsIndexDataExtentName,
        &FlatBuffers::ExtentNameUnionUnion::AsIndexHeaderExtentName,
        &FlatBuffers::ExtentNameUnionUnion::AsLogExtentName
    );
}

std::weak_ordering operator<=>(
    const FlatBuffers::DatabaseHeaderExtentNameT& left,
    const FlatBuffers::DatabaseHeaderExtentNameT& right
    )
{
    return compare(
        left,
        right,
        &FlatBuffers::DatabaseHeaderExtentNameT::header_copy_number
    );
}

std::weak_ordering operator<=>(
    const FlatBuffers::IndexDataExtentNameT& left,
    const FlatBuffers::IndexDataExtentNameT& right
    )
{
    return compare(
        left,
        right,
        &FlatBuffers::IndexDataExtentNameT::index_extent_name
    );
}

std::weak_ordering operator<=>(
    const FlatBuffers::IndexExtentNameT& left,
    const FlatBuffers::IndexExtentNameT& right
    )
{
    return compare(
        left,
        right,
        &FlatBuffers::IndexExtentNameT::index_name,
        &FlatBuffers::IndexExtentNameT::index_number,
        &FlatBuffers::IndexExtentNameT::level,
        &FlatBuffers::IndexExtentNameT::partition_number
    );
}

std::weak_ordering operator<=>(
    const FlatBuffers::IndexHeaderExtentNameT& left,
    const FlatBuffers::IndexHeaderExtentNameT& right
    )
{
    return compare(
        left,
        right,
        &FlatBuffers::IndexHeaderExtentNameT::index_extent_name
    );
}

std::weak_ordering operator<=>(
    const FlatBuffers::LogExtentNameT& left,
    const FlatBuffers::LogExtentNameT& right
    )
{
    return compare(
        left,
        right,
        &FlatBuffers::LogExtentNameT::log_extent_sequence_number
    );
}

}

}