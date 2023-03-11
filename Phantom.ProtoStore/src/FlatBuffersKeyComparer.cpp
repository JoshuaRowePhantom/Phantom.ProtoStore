#include "StandardTypes.h"
#include "FlatBuffersKeyComparer.h"
#include "Resources.h"
#include <compare>
#include <set>

namespace Phantom::ProtoStore
{

FlatBufferPointerKeyComparer::FlatBufferPointerKeyComparer(
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject
)
{
    // Copy the incoming schema so that we guarantee its lifetime.
    flatbuffers::FlatBufferBuilder schemaBuilder;
    auto rootOffset = flatbuffers::CopyTable(
        schemaBuilder,
        *FlatBuffersSchemas::ReflectionSchema,
        *FlatBuffersSchemas::ReflectionSchema_Schema,
        *reinterpret_cast<const flatbuffers::Table*>(flatBuffersReflectionSchema)
    );
    schemaBuilder.Finish(rootOffset);
    
    m_schemaBuffer = std::make_shared<flatbuffers::DetachedBuffer>(
        schemaBuilder.Release());

    auto schema = flatbuffers::GetRoot<::reflection::Schema>(
        m_schemaBuffer->data());
    const ::reflection::Object* object = nullptr;

    for (auto index = 0; index < flatBuffersReflectionSchema->objects()->size(); index++)
    {
        if (flatBuffersReflectionSchema->objects()->Get(index) == flatBuffersReflectionObject)
        { 
            object = flatBuffersReflectionSchema->objects()->Get(index);
        }
    }

    if (!object)
    {
        throw std::range_error("flatBuffersReflectionObject not in flatBuffersReflectionSchema->objects()");
    }

    m_rootComparer = InternalObjectComparer::GetObjectComparer(
        *m_internalComparers,
        schema,
        object
    );
}

std::weak_ordering FlatBufferPointerKeyComparer::Compare(
    const void* value1,
    const void* value2
) const
{
    return m_rootComparer->Compare(
        value1,
        value2);
}

uint64_t FlatBufferPointerKeyComparer::Hash(
    const void* value
) const
{
    crc_hash_v1_type hash;

    m_rootComparer->Hash(
        hash,
        value);

    return hash.checksum();
}

FlatBufferPointerKeyComparer::InternalObjectComparer::InternalObjectComparer()
{}

FlatBufferPointerKeyComparer::InternalObjectComparer::InternalObjectComparer(
    ComparerMap& internalComparers,
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject
)
{
    auto objectSortOrder = GetSortOrder(
        flatBuffersReflectionObject
    );

    flatbuffers::ForAllFields(
        flatBuffersReflectionObject,
        false,
        [&](auto flatBuffersReflectionField)
    {
        auto fieldSortOrder = GetSortOrder(
            flatBuffersReflectionField
        );

        if (flatBuffersReflectionObject->is_struct())
        {
            m_comparers.push_back(
                GetFieldComparer<flatbuffers::Struct>(
                    internalComparers,
                    flatBuffersReflectionSchema,
                    flatBuffersReflectionObject,
                    flatBuffersReflectionField
                    )
                .ApplySortOrder(objectSortOrder, fieldSortOrder));
        }
        else
        {
            m_comparers.push_back(
                GetFieldComparer<flatbuffers::Table>(
                    internalComparers,
                    flatBuffersReflectionSchema,
                    flatBuffersReflectionObject,
                    flatBuffersReflectionField
                    )
                .ApplySortOrder(objectSortOrder, fieldSortOrder));
        }
    });
}

std::weak_ordering FlatBufferPointerKeyComparer::InternalObjectComparer::Compare(
    const void* value1,
    const void* value2
) const
{
    if (value1 == value2)
    {
        return std::weak_ordering::equivalent;
    }

    if (value1 == nullptr)
    {
        return std::weak_ordering::less;
    }

    if (value2 == nullptr)
    {
        return std::weak_ordering::greater;
    }

    for (auto& comparer : m_comparers)
    {
        auto result = comparer.comparerFunction(
            comparer.flatBuffersReflectionField,
            comparer.elementComparer,
            value1,
            value2);

        if (result != std::weak_ordering::equivalent)
        {
            return ApplySortOrder(
                comparer.sortOrder,
                result);
        }
    }

    return std::weak_ordering::equivalent;
}

void FlatBufferPointerKeyComparer::InternalObjectComparer::Hash(
    crc_hash_v1_type& hash,
    const void* value
) const
{
    for (auto& hasher : m_hashers)
    {
        hasher.hasherFunction(
            hash,
            hasher.flatBuffersReflectionField,
            hasher.elementComparer,
            value);
    }
}

FlatBufferPointerKeyComparer::InternalObjectComparer* FlatBufferPointerKeyComparer::InternalObjectComparer::GetObjectComparer(
    ComparerMap& internalComparers,
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject
)
{
    if (!internalComparers.contains(flatBuffersReflectionObject))
    {
        internalComparers[flatBuffersReflectionObject] = {};

        internalComparers[flatBuffersReflectionObject] = InternalObjectComparer(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject
        );
    }

    return &internalComparers[flatBuffersReflectionObject];
}

template<
    typename Container
> static FlatBufferPointerKeyComparer::InternalObjectComparer::ComparerFunction 
FlatBufferPointerKeyComparer::InternalObjectComparer::GetFieldComparer(
    ComparerMap& internalComparers,
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject,
    const ::reflection::Field* flatBuffersReflectionField
)
{
    using ::reflection::BaseType;

    switch (flatBuffersReflectionField->type()->base_type())
    {
    case BaseType::Obj:
    {
        auto fieldObject = flatBuffersReflectionSchema->objects()->Get(
            flatBuffersReflectionField->type()->index()
        );

        const InternalObjectComparer* elementObjectComparer = GetObjectComparer(
            internalComparers,
            flatBuffersReflectionSchema,
            fieldObject);

        if (fieldObject->is_struct())
        {
            return ComparerFunction
            {
                flatBuffersReflectionField,
                elementObjectComparer,
                [](
                    const ::reflection::Field* flatBuffersReflectionField,
                    const InternalObjectComparer* elementObjectComparer,
                    const void* value1,
                    const void* value2
                ) -> std::weak_ordering
            {
                auto field1 = flatbuffers::GetFieldStruct(
                    *reinterpret_cast<const Container*>(value1),
                    *flatBuffersReflectionField
                );

                auto field2 = flatbuffers::GetFieldStruct(
                    *reinterpret_cast<const Container*>(value2),
                    *flatBuffersReflectionField
                );

                return elementObjectComparer->Compare(
                    field1,
                    field2);
            }
            };
        }
        else if constexpr (std::same_as<Container, flatbuffers::Table>)
        {
            return ComparerFunction
            {
                flatBuffersReflectionField,
                elementObjectComparer,
                [](
                    const ::reflection::Field* flatBuffersReflectionField,
                    const InternalObjectComparer* elementObjectComparer,
                    const void* value1,
                    const void* value2
                ) -> std::weak_ordering
            {
                auto field1 = flatbuffers::GetFieldT(
                    *reinterpret_cast<const Container*>(value1),
                    *flatBuffersReflectionField
                );

                auto field2 = flatbuffers::GetFieldT(
                    *reinterpret_cast<const Container*>(value2),
                    *flatBuffersReflectionField
                );

                return elementObjectComparer->Compare(
                    field1,
                    field2);
            }
            };
        }

        throw std::range_error("flatBuffersReflectionField->type()->base_type()");
    }

    case BaseType::Vector:
        return GetVectorFieldComparer(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField
        );

    case BaseType::Array:
        return GetArrayFieldComparer(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField
        );

    case BaseType::String:
        if constexpr (std::same_as<Container, flatbuffers::Table>)
        {
            return GetPrimitiveFieldComparer<Container, [](auto table, auto field)
            {
                return flatbuffers::GetStringView(flatbuffers::GetFieldS(*table, *field));
            }
            >(
                flatBuffersReflectionField);
        }
        else
        {
            throw std::range_error("flatBuffersReflectionField->type()->base_type()");
        }

    case BaseType::Bool:
        return GetPrimitiveFieldComparer<Container, &GetFieldI<Container, bool>>(
            flatBuffersReflectionField
            );

    case BaseType::Byte:
        return GetPrimitiveFieldComparer<Container, &GetFieldI<Container, int8_t>>(
            flatBuffersReflectionField
            );

    case BaseType::Double:
        return GetPrimitiveFieldComparer<Container, &GetFieldF<Container, double>>(
            flatBuffersReflectionField
            );

    case BaseType::Float:
        return GetPrimitiveFieldComparer<Container, &GetFieldF<Container, float>>(
            flatBuffersReflectionField
            );

    case BaseType::Int:
        return GetPrimitiveFieldComparer<Container, &GetFieldI<Container, int32_t>>(
            flatBuffersReflectionField
            );

    case BaseType::Long:
        return GetPrimitiveFieldComparer<Container, &GetFieldI<Container, int64_t>>(
            flatBuffersReflectionField
            );

    case BaseType::Short:
        return GetPrimitiveFieldComparer<Container, &GetFieldI<Container, int16_t>>(
            flatBuffersReflectionField
            );

    case BaseType::UByte:
        return GetPrimitiveFieldComparer<Container, &GetFieldI<Container, uint8_t>>(
            flatBuffersReflectionField
            );

    case BaseType::UInt:
        return GetPrimitiveFieldComparer<Container, &GetFieldI<Container, uint32_t>>(
            flatBuffersReflectionField
            );

    case BaseType::ULong:
        return GetPrimitiveFieldComparer<Container, &GetFieldI<Container, uint64_t>>(
            flatBuffersReflectionField
            );

    case BaseType::UShort:
        return GetPrimitiveFieldComparer<Container, &GetFieldI<Container, uint16_t>>(
            flatBuffersReflectionField
            );

    case BaseType::UType:
        return GetPrimitiveFieldComparer<Container, &GetFieldI<Container, uint8_t>>(
            flatBuffersReflectionField
            );

    default:
        throw std::range_error("flatBuffersReflectionFieldType");
    }
}

template<
    typename Container,
    auto fieldRetriever
> FlatBufferPointerKeyComparer::InternalObjectComparer::ComparerFunction 
FlatBufferPointerKeyComparer::InternalObjectComparer::GetPrimitiveFieldComparer(
    const ::reflection::Field* flatBuffersReflectionField
)
{
    return ComparerFunction
    {
        flatBuffersReflectionField,
        nullptr,
        [](
            const ::reflection::Field* flatBuffersReflectionField,
            const InternalObjectComparer* elementObjectComparer,
            const void* value1,
            const void* value2
        ) -> std::weak_ordering
    {
        auto fieldValue1 = fieldRetriever(
            reinterpret_cast<const Container*>(value1),
            flatBuffersReflectionField
        );

        auto fieldValue2 = fieldRetriever(
            reinterpret_cast<const Container*>(value2),
            flatBuffersReflectionField
        );

        return ComparePrimitive(fieldValue1, fieldValue2);
    }
    };
}

template<
    typename Value
> std::weak_ordering FlatBufferPointerKeyComparer::InternalObjectComparer::ComparePrimitive(
    Value value1,
    Value value2
)
{
    auto baseResult = value1 <=> value2;
    if constexpr (std::same_as<decltype(baseResult), std::partial_ordering>)
    {
        if (baseResult == std::partial_ordering::less)
        {
            return std::weak_ordering::less;
        }
        if (baseResult == std::partial_ordering::greater)
        {
            return std::weak_ordering::greater;
        }
        if (baseResult == std::partial_ordering::equivalent)
        {
            return std::weak_ordering::equivalent;
        }

        // The base result was unordered.
        // That could be because either one of value1, value2, or both value1 and value2 are NAN.
        // NAN is sorted to be greater than all non-NAN
        if (std::isnan(value1))
        {
            return std::weak_ordering::greater;
        }
        if (std::isnan(value2))
        {
            return std::weak_ordering::less;
        }

        // Sort NAN by bit pattern.
        if constexpr (std::same_as<double, Value>)
        {
            uint64_t intValue1 = *reinterpret_cast<uint64_t*>(&value1);
            uint64_t intValue2 = *reinterpret_cast<uint64_t*>(&value2);
            return intValue1 <=> intValue2;
        }
        else
        {
            static_assert(std::same_as<float, Value>);
            uint32_t intValue1 = *reinterpret_cast<uint32_t*>(&value1);
            uint32_t intValue2 = *reinterpret_cast<uint32_t*>(&value2);
            return intValue1 <=> intValue2;
        }
    }
    else
    {
        return baseResult;
    }
}

FlatBufferPointerKeyComparer::InternalObjectComparer::ComparerFunction FlatBufferPointerKeyComparer::InternalObjectComparer::GetVectorFieldComparer(
    ComparerMap& internalComparers,
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject,
    const ::reflection::Field* flatBuffersReflectionField
)
{
    using ::reflection::BaseType;
    switch (flatBuffersReflectionField->type()->element())
    {
    case BaseType::Bool:
        return GetTypedVectorFieldComparer<bool>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::Byte:
        return GetTypedVectorFieldComparer<int8_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::Double:
        return GetTypedVectorFieldComparer<double>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::Float:
        return GetTypedVectorFieldComparer<float>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::Int:
        return GetTypedVectorFieldComparer<int32_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::Long:
        return GetTypedVectorFieldComparer<int64_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::Obj:

        if (flatBuffersReflectionSchema->objects()->Get(
            flatBuffersReflectionField->type()->index())->is_struct())
        {
            return GetTypedVectorFieldComparer<flatbuffers::Struct>(
                internalComparers,
                flatBuffersReflectionSchema,
                flatBuffersReflectionObject,
                flatBuffersReflectionField);
        }
        else
        {
            return GetTypedVectorFieldComparer<flatbuffers::Offset<flatbuffers::Table>>(
                internalComparers,
                flatBuffersReflectionSchema,
                flatBuffersReflectionObject,
                flatBuffersReflectionField);
        }

    case BaseType::Short:
        return GetTypedVectorFieldComparer<int16_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::String:
        return GetTypedVectorFieldComparer<flatbuffers::Offset<flatbuffers::String>>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::UByte:
        return GetTypedVectorFieldComparer<uint8_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::UInt:
        return GetTypedVectorFieldComparer<uint32_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::ULong:
        return GetTypedVectorFieldComparer<uint64_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::UShort:
        return GetTypedVectorFieldComparer<uint16_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    default:
        throw std::range_error("flatBuffersReflectionField->type()->element()");
    }
}

template<
    typename Value
> FlatBufferPointerKeyComparer::InternalObjectComparer::ComparerFunction FlatBufferPointerKeyComparer::InternalObjectComparer::GetTypedVectorFieldComparer(
    ComparerMap& internalComparers,
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject,
    const ::reflection::Field* flatBuffersReflectionField
)
{
    using reflection::BaseType;
    InternalObjectComparer* elementObjectComparer = nullptr;
    if (flatBuffersReflectionField->type()->element() == BaseType::Obj)
    {
        elementObjectComparer = GetObjectComparer(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionSchema->objects()->Get(
                flatBuffersReflectionField->type()->index()));
    }

    return ComparerFunction
    {
        flatBuffersReflectionField,
        elementObjectComparer,
        [](
            const ::reflection::Field* flatBuffersReflectionField,
            const InternalObjectComparer* elementObjectComparer,
            const void* value1,
            const void* value2
        ) -> std::weak_ordering
    {
        auto vector1 = flatbuffers::GetFieldV<Value>(
            *reinterpret_cast<const flatbuffers::Table*>(value1),
            *flatBuffersReflectionField);

        auto vector2 = flatbuffers::GetFieldV<Value>(
            *reinterpret_cast<const flatbuffers::Table*>(value2),
            *flatBuffersReflectionField);

        if (vector1 == vector2)
        {
            return std::weak_ordering::equivalent;
        }

        auto size1 = flatbuffers::VectorLength(vector1);
        auto size2 = flatbuffers::VectorLength(vector2);

        auto sizeToCompare = std::min(
            size1,
            size2
        );

        for (int32_t index = 0; index < sizeToCompare; ++index)
        {
            auto value1 = vector1->Get(index);
            auto value2 = vector2->Get(index);

            std::weak_ordering result;

            if constexpr (std::same_as<flatbuffers::Offset<flatbuffers::Table>, Value>
                || std::same_as<flatbuffers::Offset<flatbuffers::Struct>, Value>)
            {
                result = elementObjectComparer->Compare(
                    value1,
                    value2);
            }
            else if constexpr (std::same_as<flatbuffers::Offset<flatbuffers::String>, Value>)
            {
                result = ComparePrimitive(
                    value1->string_view(),
                    value2->string_view());
            }
            else
            {
                result = ComparePrimitive(
                    value1,
                    value2);
            }

            if (result != std::weak_ordering::equivalent)
            {
                return result;
            }
        }

        return size1 <=> size2;
    }
    };
}

template<
> FlatBufferPointerKeyComparer::InternalObjectComparer::ComparerFunction FlatBufferPointerKeyComparer::InternalObjectComparer::GetTypedVectorFieldComparer<
    flatbuffers::Struct
>(
    ComparerMap& internalComparers,
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject,
    const ::reflection::Field* flatBuffersReflectionField
    )
{
    using reflection::BaseType;
    InternalObjectComparer* elementObjectComparer = GetObjectComparer(
        internalComparers,
        flatBuffersReflectionSchema,
        flatBuffersReflectionSchema->objects()->Get(
            flatBuffersReflectionField->type()->index()));

    return ComparerFunction
    {
        flatBuffersReflectionField,
        elementObjectComparer,
        [](
            const ::reflection::Field* flatBuffersReflectionField,
            const InternalObjectComparer* elementObjectComparer,
            const void* value1,
            const void* value2
        ) -> std::weak_ordering
    {
        auto vector1 = flatbuffers::GetFieldAnyV(
            *reinterpret_cast<const flatbuffers::Table*>(value1),
            *flatBuffersReflectionField);

        auto vector2 = flatbuffers::GetFieldAnyV(
            *reinterpret_cast<const flatbuffers::Table*>(value2),
            *flatBuffersReflectionField);

        if (vector1 == vector2)
        {
            return std::weak_ordering::equivalent;
        }

        auto size1 = vector1 ? vector1->size() : 0;
        auto size2 = vector2 ? vector2->size() : 0;

        auto sizeToCompare = std::min(
            size1,
            size2
        )
            * flatBuffersReflectionField->type()->element_size();

        for (int32_t index = 0; index < sizeToCompare; index += flatBuffersReflectionField->type()->element_size())
        {
            auto value1 = vector1->Data() + index;
            auto value2 = vector2->Data() + index;

            std::weak_ordering result;

            result = elementObjectComparer->Compare(
                value1,
                value2);

            if (result != std::weak_ordering::equivalent)
            {
                return result;
            }
        }

        return size1 <=> size2;
    }
    };
}

FlatBufferPointerKeyComparer::InternalObjectComparer::ComparerFunction FlatBufferPointerKeyComparer::InternalObjectComparer::GetArrayFieldComparer(
    ComparerMap& internalComparers,
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject,
    const ::reflection::Field* flatBuffersReflectionField
)
{
    using ::reflection::BaseType;
    switch (flatBuffersReflectionField->type()->element())
    {
    case BaseType::Bool:
        return GetTypedArrayFieldComparer<bool>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::Byte:
        return GetTypedArrayFieldComparer<int8_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::Double:
        return GetTypedArrayFieldComparer<double>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::Float:
        return GetTypedArrayFieldComparer<float>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::Int:
        return GetTypedArrayFieldComparer<int32_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::Long:
        return GetTypedArrayFieldComparer<int64_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::Obj:

        if (flatBuffersReflectionSchema->objects()->Get(
            flatBuffersReflectionField->type()->index())->is_struct())
        {
            return GetTypedArrayFieldComparer<flatbuffers::Struct>(
                internalComparers,
                flatBuffersReflectionSchema,
                flatBuffersReflectionObject,
                flatBuffersReflectionField);
        }
        else
        {
            throw std::range_error("flatBuffersReflectionField->type()->index())->is_struct()");
        }

    case BaseType::Short:
        return GetTypedArrayFieldComparer<int16_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::UByte:
        return GetTypedArrayFieldComparer<uint8_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::UInt:
        return GetTypedArrayFieldComparer<uint32_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::ULong:
        return GetTypedArrayFieldComparer<uint64_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    case BaseType::UShort:
        return GetTypedArrayFieldComparer<uint16_t>(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject,
            flatBuffersReflectionField);

    default:
        throw std::range_error("flatBuffersReflectionField->type()->element()");
    }
}

template<
    typename Value
> FlatBufferPointerKeyComparer::InternalObjectComparer::ComparerFunction FlatBufferPointerKeyComparer::InternalObjectComparer::GetTypedArrayFieldComparer(
    ComparerMap& internalComparers,
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject,
    const ::reflection::Field* flatBuffersReflectionField
)
{
    return ComparerFunction
    {
        flatBuffersReflectionField,
        nullptr,
        [](
            const ::reflection::Field* flatBuffersReflectionField,
            const InternalObjectComparer* elementObjectComparer,
            const void* value1,
            const void* value2
        ) -> std::weak_ordering
    {
        auto array1 = flatbuffers::GetAnyFieldAddressOf<const Value>(
            *reinterpret_cast<const flatbuffers::Struct*>(value1),
            *flatBuffersReflectionField);

        auto array2 = flatbuffers::GetAnyFieldAddressOf<const Value>(
            *reinterpret_cast<const flatbuffers::Struct*>(value2),
            *flatBuffersReflectionField);

        for (int32_t index = 0; index < flatBuffersReflectionField->type()->fixed_length(); ++index)
        {
            auto value1 = *(array1 + index);
            auto value2 = *(array2 + index);

            return ComparePrimitive(
                value1,
                value2);
        }

        return std::weak_ordering::equivalent;
    }
    };
}

// Specialization for arrays of structures.
template<
> FlatBufferPointerKeyComparer::InternalObjectComparer::ComparerFunction FlatBufferPointerKeyComparer::InternalObjectComparer::GetTypedArrayFieldComparer<
    flatbuffers::Struct
>(
    ComparerMap& internalComparers,
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject,
    const ::reflection::Field* flatBuffersReflectionField
    )
{
    auto elementObjectType = flatBuffersReflectionSchema->objects()->Get(
        flatBuffersReflectionField->type()->index());

    using reflection::BaseType;
    InternalObjectComparer* elementObjectComparer = GetObjectComparer(
        internalComparers,
        flatBuffersReflectionSchema,
        elementObjectType);

    return
    {
        flatBuffersReflectionField,
        elementObjectComparer,
        [](
                    const ::reflection::Field* flatBuffersReflectionField,
                    const InternalObjectComparer* elementObjectComparer,
                    const void* value1,
                    const void* value2
                ) -> std::weak_ordering
    {
        const uint8_t* array1 = flatbuffers::GetAnyFieldAddressOf<const uint8_t>(
            *reinterpret_cast<const flatbuffers::Struct*>(value1),
            *flatBuffersReflectionField);

        const uint8_t* array2 = flatbuffers::GetAnyFieldAddressOf<const uint8_t>(
            *reinterpret_cast<const flatbuffers::Struct*>(value2),
            *flatBuffersReflectionField);

        for (int32_t index = 0; index < flatBuffersReflectionField->type()->fixed_length(); ++index)
        {
            auto value1 = array1 + index * flatBuffersReflectionField->type()->element_size();
            auto value2 = array2 + index * flatBuffersReflectionField->type()->element_size();

            elementObjectComparer->Compare(
                value1,
                value2);
        }

        return std::weak_ordering::equivalent;
    }
    };
}

template<
    typename Container,
    typename Value
> Value FlatBufferPointerKeyComparer::InternalObjectComparer::GetFieldI(
    const Container* container,
    const ::reflection::Field* flatBuffersReflectionField
)
{
    if constexpr (std::same_as<flatbuffers::Table, Container>)
    {
        return flatbuffers::GetFieldI<Value>(
            *container,
            *flatBuffersReflectionField);
    }
    else
    {
        static_assert(std::same_as<flatbuffers::Struct, Container>);
        return *flatbuffers::GetAnyFieldAddressOf<const Value>(
            *container,
            *flatBuffersReflectionField);
    }
}

template<
    typename Container,
    typename Value
> Value FlatBufferPointerKeyComparer::InternalObjectComparer::GetFieldF(
    const Container* container,
    const ::reflection::Field* flatBuffersReflectionField
)
{
    if constexpr (std::same_as<flatbuffers::Table, Container>)
    {
        return flatbuffers::GetFieldF<Value>(
            *container,
            *flatBuffersReflectionField);
    }
    else
    {
        static_assert(std::same_as<flatbuffers::Struct, Container>);
        return *flatbuffers::GetAnyFieldAddressOf<const Value>(
            *container,
            *flatBuffersReflectionField);
    }
}

SortOrder FlatBufferPointerKeyComparer::InternalObjectComparer::GetSortOrder(
    const flatbuffers::Vector<flatbuffers::Offset<reflection::KeyValue>>* attributes
)
{
    if (attributes)
    {
        for (auto attribute : *attributes)
        {
            if (attribute->key()->string_view() == "SortOrder"
                && attribute->value()->string_view() == "Descending")
            {
                return SortOrder::Descending;
            }
        }
    }

    return SortOrder::Ascending;
}

SortOrder FlatBufferPointerKeyComparer::InternalObjectComparer::GetSortOrder(
    const ::reflection::Object* flatBuffersReflectionObject
)
{
    return GetSortOrder(flatBuffersReflectionObject->attributes());
}

SortOrder FlatBufferPointerKeyComparer::InternalObjectComparer::GetSortOrder(
    const ::reflection::Field* flatBuffersReflectionField
)
{
    return GetSortOrder(flatBuffersReflectionField->attributes());
}


FlatBufferKeyComparer::FlatBufferKeyComparer(
    FlatBufferPointerKeyComparer comparer
) : m_comparer{ std::move(comparer) }
{}

std::weak_ordering FlatBufferKeyComparer::CompareImpl(
    const ProtoValue& value1,
    const ProtoValue& value2
) const
{
    auto table1 = value1.as_table_if();
    auto table2 = value2.as_table_if();

    return m_comparer.Compare(
        table1,
        table2
    );
}

uint64_t FlatBufferKeyComparer::Hash(
    const ProtoValue& value
) const
{
    auto table = value.as_table_if();
    return m_comparer.Hash(table);
}

std::shared_ptr<KeyComparer> MakeFlatBufferKeyComparer(
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject)
{
    return std::make_shared<FlatBufferKeyComparer>(
        FlatBufferPointerKeyComparer
        {
            flatBuffersReflectionSchema,
            flatBuffersReflectionObject
        });
}

}
