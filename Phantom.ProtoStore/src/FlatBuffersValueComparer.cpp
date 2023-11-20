#include "StandardTypes.h"
#include "FlatBuffersValueComparer.h"
#include "Phantom.ProtoStore/numeric_cast.h"
#include "Resources.h"
#include <compare>
#include <set>

namespace Phantom::ProtoStore
{

FlatBufferPointerValueComparer::FlatBufferPointerValueComparer(
    shared_ptr<const FlatBuffersObjectSchema> flatBuffersObjectSchema
) : 
    m_flatBuffersObjectSchema(
        std::move(flatBuffersObjectSchema))
{
    m_rootComparer = InternalObjectComparer::GetObjectComparer(
        *m_internalComparers,
        m_flatBuffersObjectSchema->Schema,
        m_flatBuffersObjectSchema->Object
    );
}

std::weak_ordering FlatBufferPointerValueComparer::Compare(
    const void* value1,
    const void* value2,
    uint16_t lastFieldId
) const
{
    return m_rootComparer->Compare(
        value1,
        value2,
        lastFieldId);
}

uint64_t FlatBufferPointerValueComparer::Hash(
    const void* value
) const
{
    return ValueBuilder::Hash(
        m_flatBuffersObjectSchema->Schema,
        m_flatBuffersObjectSchema->Object,
        reinterpret_cast<const flatbuffers::Table*>(value));
}

const std::shared_ptr<const FlatBuffersObjectSchema>& FlatBufferPointerValueComparer::Schema() const
{
    return m_flatBuffersObjectSchema;
}

FlatBufferPointerValueComparer::InternalObjectComparer::InternalObjectComparer()
{}

FlatBufferPointerValueComparer::InternalObjectComparer::InternalObjectComparer(
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

std::weak_ordering FlatBufferPointerValueComparer::InternalObjectComparer::Compare(
    const void* value1,
    const void* value2,
    uint16_t lastFieldId
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

    for (auto fieldIndex = 0; fieldIndex < m_comparers.size(); fieldIndex++)
    {
        // The actual field id is fieldIndex + 1.
        // This does an inclusive comparison against the lastFieldId
        // such that asking for lastFieldId 0 always returns equivalent,
        // asking for lastFieldId:1 requires fieldIndex:0 == fieldId:1 to match.
        if (fieldIndex >= lastFieldId)
        {
            return std::weak_ordering::equivalent;
        }

        auto& comparer = m_comparers[fieldIndex];

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

FlatBufferPointerValueComparer::InternalObjectComparer* FlatBufferPointerValueComparer::InternalObjectComparer::GetObjectComparer(
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
> static FlatBufferPointerValueComparer::InternalFieldComparer 
FlatBufferPointerValueComparer::InternalObjectComparer::GetFieldComparer(
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
                &InternalFieldComparer::CompareStructField<Container>
            };
        }
        else if constexpr (std::same_as<Container, flatbuffers::Table>)
        {
            return InternalFieldComparer
            {
                flatBuffersReflectionField,
                elementObjectComparer,
                &InternalFieldComparer::CompareTableField
            };
        }
        else
        {
            throw std::range_error("flatBuffersReflectionField->type()->base_type()");
        }
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
> FlatBufferPointerValueComparer::InternalFieldComparer 
FlatBufferPointerValueComparer::InternalObjectComparer::GetPrimitiveFieldComparer(
    const ::reflection::Field* flatBuffersReflectionField
)
{
    return InternalFieldComparer
    {
        flatBuffersReflectionField,
        nullptr,
        &InternalFieldComparer::ComparePrimitiveField<Container, fieldRetriever>
    };
}

FlatBufferPointerValueComparer::InternalFieldComparer 
FlatBufferPointerValueComparer::InternalObjectComparer::GetStringFieldComparer(
    const ::reflection::Field* flatBuffersReflectionField
)
{
    return InternalFieldComparer
    {
        flatBuffersReflectionField,
        nullptr,
        &InternalFieldComparer::CompareStringField
    };
}

FlatBufferPointerValueComparer::InternalFieldComparer
FlatBufferPointerValueComparer::InternalObjectComparer::GetUnionFieldComparer(
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
        &InternalFieldComparer::CompareUnionField
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
            &InternalFieldComparer::CompareUnionFieldValue
        };

        comparer.unionComparers[numeric_cast(typeEnumerationValue->value())] = unionFieldComparer;
    }

    comparer.unionComparers[0] =
        InternalFieldComparer
    {
        nullptr,
        nullptr,
        &InternalFieldComparer::CompareEmptyUnionField
    };

    return comparer;
}


std::weak_ordering FlatBufferPointerValueComparer::InternalFieldComparer::CompareUnionField(
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

std::weak_ordering FlatBufferPointerValueComparer::InternalFieldComparer::CompareEmptyUnionField(
    const void* value1,
    const void* value2
) const
{
    std::ignore = value1;
    std::ignore = value2;
    // Do nothing.
    return std::weak_ordering::equivalent;
}

std::weak_ordering FlatBufferPointerValueComparer::InternalFieldComparer::CompareUnionFieldValue(
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

template<
    typename Value
> std::weak_ordering FlatBufferPointerValueComparer::InternalFieldComparer::ComparePrimitive(
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

FlatBufferPointerValueComparer::InternalFieldComparer 
FlatBufferPointerValueComparer::InternalObjectComparer::GetVectorFieldComparer(
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
> FlatBufferPointerValueComparer::InternalFieldComparer 
FlatBufferPointerValueComparer::InternalObjectComparer::GetTypedVectorFieldComparer(
    ComparerMap& internalComparers,
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject,
    const ::reflection::Field* flatBuffersReflectionField
)
{
    std::ignore = flatBuffersReflectionObject;

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
        flatBuffersReflectionField->type()->element_size(),
    };
}

FlatBufferPointerValueComparer::InternalFieldComparer 
FlatBufferPointerValueComparer::InternalObjectComparer::GetArrayFieldComparer(
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
> FlatBufferPointerValueComparer::InternalFieldComparer 
FlatBufferPointerValueComparer::InternalObjectComparer::GetTypedArrayFieldComparer(
    ComparerMap& internalComparers,
    const ::reflection::Schema* flatBuffersReflectionSchema,
    const ::reflection::Object* flatBuffersReflectionObject,
    const ::reflection::Field* flatBuffersReflectionField
)
{
    std::ignore = flatBuffersReflectionObject;
    
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
        flatBuffersReflectionField->type()->element_size(),
        flatBuffersReflectionField->type()->fixed_length(),
    };
}

template<
    typename Container,
    typename Value
> Value FlatBufferPointerValueComparer::InternalObjectComparer::GetFieldI(
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
> Value FlatBufferPointerValueComparer::InternalObjectComparer::GetFieldF(
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

SortOrder FlatBufferPointerValueComparer::InternalObjectComparer::GetSortOrder(
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

SortOrder FlatBufferPointerValueComparer::InternalObjectComparer::GetSortOrder(
    const ::reflection::Object* flatBuffersReflectionObject
)
{
    return GetSortOrder(flatBuffersReflectionObject->attributes());
}

SortOrder FlatBufferPointerValueComparer::InternalObjectComparer::GetSortOrder(
    const ::reflection::Field* flatBuffersReflectionField
)
{
    return GetSortOrder(flatBuffersReflectionField->attributes());
}


FlatBufferValueComparer::FlatBufferValueComparer(
    FlatBufferPointerValueComparer comparer
) : 
    m_comparer{ std::move(comparer) },
    m_prototypeValueBuilder{}
{
    m_prototypeValueBuilder.AddSchema(
        m_comparer.Schema()->Schema);
}

std::weak_ordering FlatBufferValueComparer::CompareImpl(
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

bool FlatBufferValueComparer::IsPrefixOf(
    const Prefix& prefix,
    const ProtoValue& value
) const
{
    auto table1 = prefix.Key.as_table_if();
    auto table2 = value.as_table_if();

    return std::weak_ordering::equivalent == m_comparer.Compare(
        table1,
        table2,
        prefix.LastFieldId
    );
}

uint64_t FlatBufferValueComparer::Hash(
    const ProtoValue& value
) const
{
    auto table = value.as_table_if();
    return ValueBuilder::Hash(
        m_comparer.Schema()->Schema,
        m_comparer.Schema()->Object,
        table);
}

std::shared_ptr<ValueComparer> MakeFlatBufferValueComparer(
    shared_ptr<const FlatBuffersObjectSchema> flatBuffersObjectSchema)
{
    return std::make_shared<FlatBufferValueComparer>(
        FlatBufferPointerValueComparer
        {
            std::move(flatBuffersObjectSchema)
        });
}


template<
    typename Container
>
std::weak_ordering FlatBufferPointerValueComparer::InternalFieldComparer::CompareStructField(
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

std::weak_ordering FlatBufferPointerValueComparer::InternalFieldComparer::CompareTableField(
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

template<
    typename Container,
    auto fieldRetriever
>
std::weak_ordering FlatBufferPointerValueComparer::InternalFieldComparer::ComparePrimitiveField(
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

std::weak_ordering FlatBufferPointerValueComparer::InternalFieldComparer::CompareStringField(
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
    typename Value
>
std::weak_ordering FlatBufferPointerValueComparer::InternalFieldComparer::CompareVectorField(
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

    for (size_t index = 0; index < sizeToCompare; ++index)
    {
        auto vectorValue1 = vector1->Get(numeric_cast(index));
        auto vectorValue2 = vector2->Get(numeric_cast(index));

        std::weak_ordering result;

        if constexpr (std::same_as<flatbuffers::Offset<flatbuffers::Table>, Value>
            || std::same_as<flatbuffers::Offset<flatbuffers::Struct>, Value>)
        {
            result = elementObjectComparer->Compare(
                vectorValue1,
                vectorValue2);
        }
        else if constexpr (std::same_as<flatbuffers::Offset<flatbuffers::String>, Value>)
        {
            result = ComparePrimitive(
                vectorValue1->string_view(),
                vectorValue2->string_view());
        }
        else
        {
            result = ComparePrimitive(
                vectorValue1,
                vectorValue2);
        }

        if (result != std::weak_ordering::equivalent)
        {
            return result;
        }
    }

    return size1 <=> size2;
}

template<
>
std::weak_ordering FlatBufferPointerValueComparer::InternalFieldComparer::CompareVectorField<flatbuffers::Struct>(
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

    for (flatbuffers::uoffset_t index = 0; index < sizeToCompare; index += elementSize)
    {
        auto vectorValue1 = vector1->Data() + index;
        auto vectorValue2 = vector2->Data() + index;

        std::weak_ordering result;

        result = elementObjectComparer->Compare(
            vectorValue1,
            vectorValue2);

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
std::weak_ordering FlatBufferPointerValueComparer::InternalFieldComparer::CompareArrayField(
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

    for (uint32_t index = 0; index < fixedLength; ++index)
    {
        auto arrayValue1 = *(array1 + index);
        auto arrayValue2 = *(array2 + index);

        auto result = ComparePrimitive(
            arrayValue1,
            arrayValue2);

        if (result != std::weak_ordering::equivalent)
        {
            return result;
        }
    }

    return std::weak_ordering::equivalent;
}

template<
>
std::weak_ordering FlatBufferPointerValueComparer::InternalFieldComparer::CompareArrayField<
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
        auto arrayValue1 = array1 + index * elementSize;
        auto arrayValue2 = array2 + index * elementSize;

        elementObjectComparer->Compare(
            arrayValue1,
            arrayValue2);
    }

    return std::weak_ordering::equivalent;

}

ValueComparer::BuildValueResult FlatBufferValueComparer::BuildValue(
    ValueBuilder& valueBuilder,
    const ProtoValue& value
) const
{
    if (m_comparer.Schema()->MessageEncodingOptions == FlatBuffers::FlatBuffersMessageEncodingOptions::SerializedByteMessage)
    {
        return BuildDataValue(
            valueBuilder,
            value);
    }

    if (m_comparer.Schema()->GraphEncodingOptions == FlatBuffers::FlatBuffersGraphEncodingOptions::NoDuplicateDetection)
    {
        auto deduplicateStrings = m_comparer.Schema()->StringEncodingOptions == FlatBuffers::FlatBuffersStringEncodingOptions::ShareStrings;

        return flatbuffers::Offset<FlatBuffers::ValuePlaceholder>
        {
            flatbuffers::CopyTable(
                valueBuilder.builder(),
                *m_comparer.Schema()->Schema,
                *m_comparer.Schema()->Object,
                *value.as_table_if(),
                deduplicateStrings).o
        };
    }

    return flatbuffers::Offset<FlatBuffers::ValuePlaceholder>
    {
        valueBuilder.CopyTableDag(
            m_comparer.Schema()->Schema,
            m_comparer.Schema()->Object,
            value.as_table_if()
            ).o
    };
}

flatbuffers::Offset<FlatBuffers::DataValue> FlatBufferValueComparer::BuildDataValue(
    ValueBuilder& valueBuilder,
    const ProtoValue& value
) const
{
    if (!value.has_value())
    {
        return 0;
    }

    if (value.as_aligned_message_if())
    {
        return valueBuilder.CreateDataValue(
            value.as_aligned_message_if());
    }

    if (value.as_table_if())
    {
        flatbuffers::FlatBufferBuilder builder;

        auto rootOffset = flatbuffers::CopyTable(
            builder,
            *m_comparer.Schema()->Schema,
            *m_comparer.Schema()->Object,
            *value.as_table_if());

        builder.Finish(rootOffset);

        return valueBuilder.CreateDataValue(
            AlignedMessage{ builder }
        );
    }

    throw std::range_error("value is not a flatbuffer value");
}

size_t FlatBufferValueComparer::GetEstimatedSize(
    const ProtoValue& value
) const
{
    return ValueBuilder::GetEstimatedSize(
        m_comparer.Schema()->Schema,
        m_comparer.Schema()->Object,
        value.as_table_if());
}

}
