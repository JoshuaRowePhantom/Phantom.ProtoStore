#include "StandardTypes.h"
#include "src/ProtoStoreInternal_generated.h"

namespace Phantom::ProtoStore::FlatBuffers
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
        auto rightValue = (left.*accessor)();

        if (!leftValue && !rightValue)
        {
            return std::weak_ordering::equivalent;
        }
        if (leftValue)
        {
            return std::weak_ordering::less;
        }
        if (rightValue)
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
    OtherObject Object::*primitiveField
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
    
    (
        ...
        &&
        ((result = compare(left, right, std::forward<Comparers>(comparers))) == std::weak_ordering::equivalent)
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
    const FlatBuffers::IndexHeaderExtentName& left,
    const FlatBuffers::IndexHeaderExtentName& right
    )
{
    return compare(
        left,
        right,
        &FlatBuffers::IndexHeaderExtentName::index_extent_name
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