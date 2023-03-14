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
    hash_v1_type hash;

    m_rootComparer->Hash(
        hash,
        value);

    auto checksum = hash.checksum();

    return checksum;
}

KeyComparer::BuildValueResult FlatBufferKeyComparer::BuildValue(
    ValueBuilder& valueBuilder,
    const ProtoValue& value
) const
{
    return valueBuilder.CreateDataValue(
        value.as_aligned_message_if());
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

    if (flatBuffersReflectionObject->is_struct())
    {
        m_hashers.push_back(
            InternalFieldComparer
            {
                nullptr,
                nullptr,
                {},
                &InternalFieldComparer::HashStructObject,
                static_cast<uint32_t>(flatBuffersReflectionObject->bytesize()),
            }
        );
    }
    else
    {
        m_hashers = m_comparers;
    }
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
        auto result = (comparer.*comparer.comparerFunction)(
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
    hash_v1_type& hash,
    const void* value
) const
{
    if (!value)
    {
        HashPrimitive(hash, 0);
        return;
    }

    for (auto& hasher : m_hashers)
    {
        (hasher.*hasher.hasherFunction)(
            hash,
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
> static FlatBufferPointerKeyComparer::InternalFieldComparer 
FlatBufferPointerKeyComparer::InternalObjectComparer::GetFieldComparer(
    ComparerMap& internalComparers,
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject,
    const ::reflection::Field* flatBuffersReflectionField
)
{
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
            return InternalFieldComparer
            {
                flatBuffersReflectionField,
                elementObjectComparer,
                &InternalFieldComparer::CompareStructField<Container>,
                &InternalFieldComparer::HashStructField<Container>
            };
        }
        else if constexpr (std::same_as<Container, flatbuffers::Table>)
        {
            return InternalFieldComparer
            {
                flatBuffersReflectionField,
                elementObjectComparer,
                &InternalFieldComparer::CompareTableField,
                &InternalFieldComparer::HashTableField
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
            return GetStringFieldComparer(
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

    case BaseType::Union:
        if constexpr (std::same_as<Container, flatbuffers::Table>)
        {
            return GetUnionFieldComparer(
                internalComparers,
                flatBuffersReflectionSchema,
                flatBuffersReflectionObject,
                flatBuffersReflectionField);
        }
        else
        {
            throw std::range_error("flatBuffersReflectionField->type()->base_type()");
        }

    default:
        throw std::range_error("flatBuffersReflectionFieldType");
    }
}

template<
    typename Container,
    auto fieldRetriever
> FlatBufferPointerKeyComparer::InternalFieldComparer 
FlatBufferPointerKeyComparer::InternalObjectComparer::GetPrimitiveFieldComparer(
    const ::reflection::Field* flatBuffersReflectionField
)
{
    return InternalFieldComparer
    {
        flatBuffersReflectionField,
        nullptr,
        &InternalFieldComparer::ComparePrimitiveField<Container, fieldRetriever>,
        &InternalFieldComparer::HashPrimitiveField<Container, fieldRetriever>
    };
}

FlatBufferPointerKeyComparer::InternalFieldComparer 
FlatBufferPointerKeyComparer::InternalObjectComparer::GetStringFieldComparer(
    const ::reflection::Field* flatBuffersReflectionField
)
{
    return InternalFieldComparer
    {
        flatBuffersReflectionField,
        nullptr,
        &InternalFieldComparer::CompareStringField,
        &InternalFieldComparer::HashStringField
    };
}

FlatBufferPointerKeyComparer::InternalFieldComparer
FlatBufferPointerKeyComparer::InternalObjectComparer::GetUnionFieldComparer(
    ComparerMap& internalComparers,
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject,
    const ::reflection::Field* flatBuffersReflectionField
)
{
    auto typeField = std::find_if(
        flatBuffersReflectionObject->fields()->begin(),
        flatBuffersReflectionObject->fields()->end(),
        [&](const ::reflection::Field* field)
    {
        return field->id() == flatBuffersReflectionField->id() - 1;
    });

    auto typeEnumeration = flatBuffersReflectionSchema->enums()->Get(
        typeField->type()->index()
    );
    if (typeEnumeration->underlying_type()->base_type() != BaseType::UType
        || typeEnumeration->underlying_type()->base_size() != 1)
    {
        throw std::range_error("Union type discriminator must be UByte");
    }

    auto comparer = InternalFieldComparer
    {
        *typeField,
        nullptr,
        &InternalFieldComparer::CompareUnionField,
        &InternalFieldComparer::HashUnionField
    };

    for (auto typeEnumerationValue : *typeEnumeration->values())
    {
        // Skip the "NONE" value.
        if (typeEnumerationValue->value() == 0)
        {
            continue;
        }

        auto unionValueType = typeEnumerationValue->union_type();
        if (unionValueType->base_type() != BaseType::Obj)
        {
            throw std::range_error("Unions must be of table type");
        }
        
        auto unionTableType = flatBuffersReflectionSchema->objects()->Get(
            unionValueType->index());
        if (unionTableType->is_struct())
        {
            throw std::range_error("Unions must be of table type");
        }

        auto unionComparer = GetObjectComparer(
            internalComparers,
            flatBuffersReflectionSchema,
            unionTableType);

        auto unionFieldComparer = InternalFieldComparer
        {
            flatBuffersReflectionField,
            unionComparer,
            &InternalFieldComparer::CompareUnionFieldValue,
            &InternalFieldComparer::HashUnionFieldValue,
        };

        comparer.unionComparers[typeEnumerationValue->value()] = unionFieldComparer;
    }

    comparer.unionComparers[0] =
        InternalFieldComparer
    {
        nullptr,
        nullptr,
        &InternalFieldComparer::CompareEmptyUnionField,
        &InternalFieldComparer::HashEmptyUnionField
    };

    return comparer;
}


std::weak_ordering FlatBufferPointerKeyComparer::InternalFieldComparer::CompareUnionField(
    const void* value1,
    const void* value2
) const
{
    // The enumeration type field will have already been compared,
    // so we don't need to worry about the values being of different types.
    auto unionType = flatbuffers::GetFieldI<uint8_t>(
        *reinterpret_cast<const ::flatbuffers::Table*>(value1),
        *flatBuffersReflectionField
    );

    auto& comparer = unionComparers.at(unionType);
    
    return (comparer.*comparer.comparerFunction)(
        value1,
        value2
    );
}

void FlatBufferPointerKeyComparer::InternalFieldComparer::HashUnionField(
    hash_v1_type& hash,
    const void* value
) const
{
    auto unionType = flatbuffers::GetFieldI<uint8_t>(
        *reinterpret_cast<const ::flatbuffers::Table*>(value),
        *flatBuffersReflectionField
    );

    auto& comparer = unionComparers.at(unionType);

    return (comparer.*comparer.hasherFunction)(
        hash,
        value
    );
}

std::weak_ordering FlatBufferPointerKeyComparer::InternalFieldComparer::CompareEmptyUnionField(
    const void* value1,
    const void* value2
) const
{
    // Do nothing.
    return std::weak_ordering::equivalent;
}

void FlatBufferPointerKeyComparer::InternalFieldComparer::HashEmptyUnionField(
    hash_v1_type& hash,
    const void* value
) const
{
    // Do nothing.
}

std::weak_ordering FlatBufferPointerKeyComparer::InternalFieldComparer::CompareUnionFieldValue(
    const void* value1,
    const void* value2
) const
{
    auto fieldValue1 = flatbuffers::GetFieldT(
        *reinterpret_cast<const ::flatbuffers::Table*>(value1),
        *flatBuffersReflectionField
    );

    auto fieldValue2 = flatbuffers::GetFieldT(
        *reinterpret_cast<const ::flatbuffers::Table*>(value2),
        *flatBuffersReflectionField
    );

    return elementObjectComparer->Compare(
        fieldValue1,
        fieldValue2);
}

void FlatBufferPointerKeyComparer::InternalFieldComparer::HashUnionFieldValue(
    hash_v1_type& hash,
    const void* value
) const
{
    auto fieldValue = flatbuffers::GetFieldT(
        *reinterpret_cast<const ::flatbuffers::Table*>(value),
        *flatBuffersReflectionField
    );

    elementObjectComparer->Hash(
        hash,
        fieldValue);
}

template<
    typename Value
> std::weak_ordering FlatBufferPointerKeyComparer::InternalFieldComparer::ComparePrimitive(
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

template<
    typename Value
> void FlatBufferPointerKeyComparer::HashPrimitive(
    hash_v1_type& hash,
    Value value
)
{
    auto span = as_bytes(value);

    hash.process_bytes(
        span.data(),
        span.size()
    );
}

FlatBufferPointerKeyComparer::InternalFieldComparer 
FlatBufferPointerKeyComparer::InternalObjectComparer::GetVectorFieldComparer(
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
> FlatBufferPointerKeyComparer::InternalFieldComparer 
FlatBufferPointerKeyComparer::InternalObjectComparer::GetTypedVectorFieldComparer(
    ComparerMap& internalComparers,
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject,
    const ::reflection::Field* flatBuffersReflectionField
)
{
    InternalObjectComparer* elementObjectComparer = nullptr;
    
    if (flatBuffersReflectionField->type()->element() == reflection::BaseType::Obj)
    {
        elementObjectComparer = GetObjectComparer(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionSchema->objects()->Get(
                flatBuffersReflectionField->type()->index()));
    }

    return InternalFieldComparer
    {
        flatBuffersReflectionField,
        elementObjectComparer,
        &InternalFieldComparer::CompareVectorField<Value>,
        &InternalFieldComparer::HashVectorField<Value>,
        flatBuffersReflectionField->type()->element_size(),
    };
}

FlatBufferPointerKeyComparer::InternalFieldComparer 
FlatBufferPointerKeyComparer::InternalObjectComparer::GetArrayFieldComparer(
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
> FlatBufferPointerKeyComparer::InternalFieldComparer 
FlatBufferPointerKeyComparer::InternalObjectComparer::GetTypedArrayFieldComparer(
    ComparerMap& internalComparers,
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject,
    const ::reflection::Field* flatBuffersReflectionField
)
{
    InternalObjectComparer* elementObjectComparer = nullptr;

    if (flatBuffersReflectionField->type()->element() == reflection::BaseType::Obj)
    {
        elementObjectComparer = GetObjectComparer(
            internalComparers,
            flatBuffersReflectionSchema,
            flatBuffersReflectionSchema->objects()->Get(
                flatBuffersReflectionField->type()->index()));
    }

    return InternalFieldComparer
    {
        flatBuffersReflectionField,
        elementObjectComparer,
        &InternalFieldComparer::CompareArrayField<Value>,
        // No need to hash array fields, since they are hashed as part
        // of the parent struct.
        nullptr,
        flatBuffersReflectionField->type()->element_size(),
        flatBuffersReflectionField->type()->fixed_length(),
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


void FlatBufferPointerKeyComparer::InternalFieldComparer::HashStructObject(
    hash_v1_type& hash,
    const void* value
) const
{
    auto span = std::span<const std::byte>
    {
        reinterpret_cast<const std::byte*>(value),
        elementSize
    };

    HashPrimitive(hash, span);
}

template<
    typename Container
>
std::weak_ordering FlatBufferPointerKeyComparer::InternalFieldComparer::CompareStructField(
    const void* value1,
    const void* value2
) const
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

template<
    typename Container
>
void FlatBufferPointerKeyComparer::InternalFieldComparer::HashStructField(
    hash_v1_type& hash,
    const void* value
) const
{
    auto field = flatbuffers::GetFieldStruct(
        *reinterpret_cast<const Container*>(value),
        *flatBuffersReflectionField
    );

    return elementObjectComparer->Hash(
        hash,
        field);
}

std::weak_ordering FlatBufferPointerKeyComparer::InternalFieldComparer::CompareTableField(
    const void* value1,
    const void* value2
) const
{
    auto field1 = flatbuffers::GetFieldT(
        *reinterpret_cast<const flatbuffers::Table*>(value1),
        *flatBuffersReflectionField
    );

    auto field2 = flatbuffers::GetFieldT(
        *reinterpret_cast<const flatbuffers::Table*>(value2),
        *flatBuffersReflectionField
    );

    return elementObjectComparer->Compare(
        field1,
        field2);
}

void FlatBufferPointerKeyComparer::InternalFieldComparer::HashTableField(
    hash_v1_type& hash,
    const void* value
) const
{
    auto field = flatbuffers::GetFieldT(
        *reinterpret_cast<const flatbuffers::Table*>(value),
        *flatBuffersReflectionField
    );

    return elementObjectComparer->Hash(
        hash,
        field);
}

template<
    typename Container,
    auto fieldRetriever
>
std::weak_ordering FlatBufferPointerKeyComparer::InternalFieldComparer::ComparePrimitiveField(
    const void* value1,
    const void* value2
) const
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

std::weak_ordering FlatBufferPointerKeyComparer::InternalFieldComparer::CompareStringField(
    const void* value1,
    const void* value2
) const
{
    auto fieldValue1 = flatbuffers::GetStringView(
        flatbuffers::GetFieldS(
            *reinterpret_cast<const flatbuffers::Table*>(value1),
            *flatBuffersReflectionField));

    auto fieldValue2 = flatbuffers::GetStringView(
        flatbuffers::GetFieldS(
            *reinterpret_cast<const flatbuffers::Table*>(value2),
            *flatBuffersReflectionField));

    return fieldValue1 <=> fieldValue2;
}

template<
    typename Container,
    auto fieldRetriever
>
void FlatBufferPointerKeyComparer::InternalFieldComparer::HashPrimitiveField(
    hash_v1_type& hash,
    const void* value
) const
{
    auto fieldValue = fieldRetriever(
        reinterpret_cast<const Container*>(value),
        flatBuffersReflectionField
    );

    HashPrimitive(
        hash,
        fieldValue);
}

void FlatBufferPointerKeyComparer::InternalFieldComparer::HashStringField(
    hash_v1_type& hash,
    const void* value
) const
{
    auto stringView = flatbuffers::GetStringView(
        flatbuffers::GetFieldS(
            *reinterpret_cast<const flatbuffers::Table*>(value),
            *flatBuffersReflectionField));

    auto span = std::span<const char>{ stringView };
    if (!span.data())
    {
        HashPrimitive(hash, 0);
    }
    else
    {
        HashPrimitive(
            hash,
            span);
    }
}

template<
    typename Value
>
std::weak_ordering FlatBufferPointerKeyComparer::InternalFieldComparer::CompareVectorField(
    const void* value1,
    const void* value2
) const
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

template<
    typename Value
>
void FlatBufferPointerKeyComparer::InternalFieldComparer::HashVectorField(
    hash_v1_type& hash,
    const void* value
) const
{
    auto vector = flatbuffers::GetFieldV<Value>(
        *reinterpret_cast<const flatbuffers::Table*>(value),
        *flatBuffersReflectionField);

    if (!vector)
    {
        HashPrimitive(hash, 0);
        return;
    }

    HashPrimitive(
        hash,
        vector->size());

    if constexpr (std::same_as<flatbuffers::Offset<flatbuffers::Table>, Value>)
    {
        for (auto table : *vector)
        {
            elementObjectComparer->Hash(
                hash,
                table);
        }
    }
    else if constexpr (std::same_as<flatbuffers::Offset<flatbuffers::String>, Value>)
    {
        for (auto string : *vector)
        {
            auto string_view = string->string_view();
            auto span = std::span
            {
                string_view.data(),
                string_view.size(),
            };

            HashPrimitive(
                hash,
                string_view.size());

            HashPrimitive(
                hash,
                span);
        }
    }
    else
    {
        // It's a vector of primitive values.
        // Hash the entire vector.
        auto span = std::span<Value>
        {
            vector->data(),
            vector->size()
        };

        HashPrimitive(
            hash,
            span
        );
    }
}


template<
>
std::weak_ordering FlatBufferPointerKeyComparer::InternalFieldComparer::CompareVectorField<flatbuffers::Struct>(
    const void* value1,
    const void* value2
) const
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
        * elementSize;

    for (int32_t index = 0; index < sizeToCompare; index += elementSize)
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
template<
>
void FlatBufferPointerKeyComparer::InternalFieldComparer::HashVectorField<flatbuffers::Struct>(
    hash_v1_type& hash,
    const void* value
) const
{
    auto vector = flatbuffers::GetFieldAnyV(
        *reinterpret_cast<const flatbuffers::Table*>(value),
        *flatBuffersReflectionField);

    if (!vector)
    {
        HashPrimitive(hash, 0);
        return;
    }

    HashPrimitive(
        hash,
        vector->size());

    // It's a vector of struct values.
    // Hash the entire vector.
    auto span = std::span
    {
        vector->Data(),
        vector->size() * elementSize
    };

    HashPrimitive(
        hash,
        span
    );
}

template<
    typename Value
>
std::weak_ordering FlatBufferPointerKeyComparer::InternalFieldComparer::CompareArrayField(
    const void* value1,
    const void* value2
) const
{
    auto array1 = flatbuffers::GetAnyFieldAddressOf<const Value>(
        *reinterpret_cast<const flatbuffers::Struct*>(value1),
        *flatBuffersReflectionField);

    auto array2 = flatbuffers::GetAnyFieldAddressOf<const Value>(
        *reinterpret_cast<const flatbuffers::Struct*>(value2),
        *flatBuffersReflectionField);

    for (int32_t index = 0; index < elementSize; ++index)
    {
        auto value1 = *(array1 + index);
        auto value2 = *(array2 + index);

        return ComparePrimitive(
            value1,
            value2);
    }

    return std::weak_ordering::equivalent;
}

template<
>
std::weak_ordering FlatBufferPointerKeyComparer::InternalFieldComparer::CompareArrayField<
    flatbuffers::Struct
    >(
    const void* value1,
    const void* value2
) const
{
    const uint8_t* array1 = flatbuffers::GetAnyFieldAddressOf<const uint8_t>(
        *reinterpret_cast<const flatbuffers::Struct*>(value1),
        *flatBuffersReflectionField);

    const uint8_t* array2 = flatbuffers::GetAnyFieldAddressOf<const uint8_t>(
        *reinterpret_cast<const flatbuffers::Struct*>(value2),
        *flatBuffersReflectionField);

    for (int32_t index = 0; index < fixedLength; ++index)
    {
        auto value1 = array1 + index * elementSize;
        auto value2 = array2 + index * elementSize;

        elementObjectComparer->Compare(
            value1,
            value2);
    }

    return std::weak_ordering::equivalent;

}

}
