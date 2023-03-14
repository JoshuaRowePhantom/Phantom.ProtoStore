#include "KeyComparer.h"
#include "Resources.h"

namespace Phantom::ProtoStore
{

// Hash computation
size_t ValueBuilder::InternedValueKeyComparer::operator()(
    const auto& key
    ) const
{
    return m_valueBuilder->Hash(key);
}

// Equality comparison
bool ValueBuilder::InternedValueKeyComparer::operator()(
    const auto& value1,
    const auto& value2
    ) const
{
    return m_valueBuilder->Equals(value1, value2);
}

size_t ValueBuilder::Hash(
    const auto& value
)
{
    if constexpr (std::same_as<const InternedValue&, decltype(value)>)
    {
        return m_internedSchemaItemsByPointer.at(value.schemaIdentifier)->hash(
            builder().GetCurrentBufferPointer() + builder().GetSize() - value.offset.o
            );
    }
    else if constexpr (std::same_as<const UninternedValue&, decltype(value)>)
    {
        return value.schemaItem->hash(value.value);
    }
    else
    {
        static_assert(std::same_as<const InterningValue&, decltype(value)>);
        return value.schemaItem->hash(
            builder().GetCurrentBufferPointer() + builder().GetSize() - value.offset.o
            );
    }
}

bool ValueBuilder::Equals(
    const auto& value1,
    const auto& value2
)
{
    const void* value1Pointer;
    const InternedSchemaItem* schemaItem;

    const void* schemaIdentifier1;
    const void* schemaIdentifier2;

    if constexpr (std::same_as<const InternedValue&, decltype(value1)>)
    {
        value1Pointer = builder().GetCurrentBufferPointer() + builder().GetSize() - value1.offset.o;
        schemaIdentifier1 = value1.schemaIdentifier;
    }
    else
    {
        value1Pointer = value1.value;
        schemaItem = value1.schemaItem;
        schemaIdentifier1 = value1.schemaItem->schemaIdentifier;
    }

    const void* value2Pointer;

    if constexpr (std::same_as<const InternedValue&, decltype(value2)>)
    {
        value2Pointer = builder().GetCurrentBufferPointer() + builder().GetSize() - value2.offset.o;
        schemaIdentifier2 = value2.schemaIdentifier;
    }
    else
    {
        value2Pointer = value2.value;
        schemaItem = value2.schemaItem;
        schemaIdentifier2 = value2.schemaItem->schemaIdentifier;
    }

    if (schemaIdentifier1 != schemaIdentifier2)
    {
        return false;
    }

    if constexpr (
        std::same_as<const InternedValue&, decltype(value1)>
        && std::same_as<const InternedValue&, decltype(value2)>
        )
    {
        schemaItem = m_internedSchemaItemsByPointer[schemaIdentifier1];
    }

    return schemaItem->equal_to(value1Pointer, value2Pointer);
}

ValueBuilder::ValueBuilder(
    flatbuffers::FlatBufferBuilder* flatBufferBuilder
) :
    m_flatBufferBuilder{ flatBufferBuilder },
    m_internedValues{ 0, InternedValueKeyComparer{ this }, InternedValueKeyComparer{ this } },
    m_internedSchemaItemsByItem{ 0, SchemaItemComparer{}, SchemaItemComparer{} }
{}

const void* ValueBuilder::SchemaItem::schemaIdentifier() const
{
    if (object)
    {
        return object;
    }

    return type;
}

ValueBuilder::InterningValue::operator ValueBuilder::InternedValue() const
{
    return
    {
        .schemaIdentifier = schemaItem->schemaIdentifier,
        .offset = offset
    };
}

const ValueBuilder::InternedSchemaItem& ValueBuilder::InternSchemaItem(
    const SchemaItem& schemaItem
)
{
    if (m_internedSchemaItemsByPointer.contains(schemaItem.schemaIdentifier()))
    {
        return *m_internedSchemaItemsByPointer[schemaItem.schemaIdentifier()];
    }

    if (m_internedSchemaItemsByItem.contains(schemaItem))
    {
        return *(m_internedSchemaItemsByPointer[schemaItem.schemaIdentifier()] = &m_internedSchemaItemsByItem[schemaItem]);
    }

    return *(
        m_internedSchemaItemsByPointer[schemaItem.schemaIdentifier()] = &(
            m_internedSchemaItemsByItem[schemaItem] = MakeInternedSchemaItem(
                schemaItem)));
}


ValueBuilder::InternedSchemaItem ValueBuilder::MakeInternedSchemaItem(
    const SchemaItem& schemaItem
)
{
    using reflection::BaseType;

    if (schemaItem.object)
    {
        return MakeInternedObjectSchemaItem(schemaItem);
    }

    if (schemaItem.type->base_type() == BaseType::Vector)
    {
        return MakeInternedVectorSchemaItem(
            schemaItem);
    }

    throw std::range_error("Unsupported type");
}

ValueBuilder::InternedSchemaItem ValueBuilder::MakeInternedVectorSchemaItem(
    const SchemaItem& schemaItem
)
{
    using reflection::BaseType;
 
    switch (schemaItem.type->element())
    {
    case BaseType::String:
        return InternedSchemaItem
        {
            .schemaIdentifier = schemaItem.schemaIdentifier(),
            .hash = [=](const void* v)
            {
                auto vector = reinterpret_cast<const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>*>(v);
                size_t hash = 0;
                for (auto i = 0; i < vector->size(); ++i)
                {
                    hash ^= std::hash<std::string_view>{}(
                        flatbuffers::GetStringView(
                            vector->Get(i)));
                }
                return hash;
            },
            .equal_to = [=](const void* a, const void* b)
            {
                auto vectorA = reinterpret_cast<const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>*>(a);
                auto vectorB = reinterpret_cast<const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>*>(b);
                if (vectorA->size() != vectorB->size())
                {
                    return false;
                }

                for (auto i = 0; i < vectorA->size(); ++i)
                {
                    if (flatbuffers::GetStringView(vectorA->Get(i)) != flatbuffers::GetStringView(vectorB->Get(i)))
                    {
                        return false;
                    }
                }

                return true;
            },
        };
        break;

    case BaseType::Obj:
    {
        auto object= schemaItem.schema->objects()->Get(schemaItem.type->index());
        if (object->is_struct())
        {
            goto TrivialType;
        }

        auto objectSchemaItem = InternSchemaItem(
            {
                schemaItem.schema,
                object
            }
        );

        return InternedSchemaItem
        {
            .schemaIdentifier = schemaItem.schemaIdentifier(),
            .hash = [=](const void* v)
            {
                auto vector = reinterpret_cast<const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::Table>>*>(v);
                size_t hash = 0;
                for (auto i = 0; i < vector->size(); ++i)
                {
                    hash ^= objectSchemaItem.hash(vector->Get(i));
                }
                return hash;
            },
            .equal_to = [=](const void* a, const void* b)
            {
                auto vectorA = reinterpret_cast<const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::Table>>*>(a);
                auto vectorB = reinterpret_cast<const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::Table>>*>(b);
                if (vectorA->size() != vectorB->size())
                {
                    return false;
                }

                for (auto i = 0; i < vectorA->size(); ++i)
                {
                    if (!objectSchemaItem.equal_to(
                        vectorA->Get(i),
                        vectorB->Get(i)))
                    {
                        return false;
                    }
                }

                return true;
            },
        };
        break;
    }

    default:
    TrivialType:
        // The vector is a trivial type.
        // We can use bitwise hashing and equality comparison.
        return InternedSchemaItem
        {
            .schemaIdentifier = schemaItem.schemaIdentifier(),
            .hash = [=](const void* v)
            {
                auto vector = reinterpret_cast<const flatbuffers::VectorOfAny*>(v);
                return std::hash<std::string_view>{}(
                    std::string_view(
                        reinterpret_cast<const char*>(vector->Data()),
                        vector->size() * schemaItem.type->element_size()));
            },
            .equal_to = [=](const void* a, const void* b)
            {
                auto vectorA = reinterpret_cast<const flatbuffers::VectorOfAny*>(a);
                auto vectorB = reinterpret_cast<const flatbuffers::VectorOfAny*>(b);
                return
                    vectorA->size() == vectorB->size()
                    &&
                    std::memcmp(
                        vectorA->Data(),
                        vectorB->Data(),
                        vectorA->size() * schemaItem.type->element_size())
                    == 0;
            },
        };
    }
}

ValueBuilder::InternedSchemaItem ValueBuilder::MakeInternedObjectSchemaItem(
    const SchemaItem& schemaItem
)
{
    auto comparers =
        FlatBuffersObjectSchema
        {
            schemaItem.schema,
            schemaItem.object
        }.MakeComparers();

    return InternedSchemaItem
    {
        .schemaIdentifier = schemaItem.schemaIdentifier(),
        .hash = [=](const void* v)
        {
            return comparers.hash(
                FlatValue{reinterpret_cast<const flatbuffers::Table*>(v)});
        },
        .equal_to = [=](const void* a, const void* b)
        {
            return comparers.equal_to(
                FlatValue{reinterpret_cast<const flatbuffers::Table*>(a)},
                FlatValue{reinterpret_cast<const flatbuffers::Table*>(b)});
        },
    };
}

flatbuffers::Offset<void> ValueBuilder::GetInternedValue(
    const SchemaItem& schemaItem,
    const void* value
)
{
    auto& internedSchemaItem = InternSchemaItem(schemaItem);
    auto internedItem = m_internedValues.find(
        UninternedValue
        {
            value,
            &internedSchemaItem
        });
    if (internedItem == m_internedValues.end())
    {
        return {0};
    }

    return internedItem->offset;
}

void ValueBuilder::InternValue(
    const SchemaItem& schemaItem,
    flatbuffers::Offset<void> offset
)
{
    auto& internedSchemaItem = InternSchemaItem(schemaItem);

    m_internedValues.insert(
        InterningValue
        {
            &internedSchemaItem,
            offset
        });
}

flatbuffers::FlatBufferBuilder& ValueBuilder::builder(
) const
{
    return *m_flatBufferBuilder;
}

flatbuffers::Offset<FlatBuffers::DataValue> ValueBuilder::CreateDataValue(
    const AlignedMessage& message
)
{
    if (!message.Payload.data())
    {
        return {};
    }

    builder().ForceVectorAlignment(
        message.Payload.size(),
        1,
        message.Alignment
    );

    auto dataVectorOffset = builder().CreateVector<int8_t>(
        get_int8_t_span(message.Payload).data(),
        message.Payload.size());

    return FlatBuffers::CreateDataValue(
        builder(),
        dataVectorOffset,
        1);
}


// Hash computation
size_t ValueBuilder::SchemaItemComparer::operator()(
    const SchemaItem& item
    ) const
{
    return 
        FlatBuffersSchemas::ReflectionSchema_SchemaComparers.hash(
            item.schema)
        ^
        FlatBuffersSchemas::ReflectionSchema_TypeComparers.hash(
            item.type)
        ^
        FlatBuffersSchemas::ReflectionSchema_ObjectComparers.hash(
            item.object);
}

// Equality computation
bool ValueBuilder::SchemaItemComparer::operator()(
    const SchemaItem& item1,
    const SchemaItem& item2
    ) const
{
    return
        FlatBuffersSchemas::ReflectionSchema_SchemaComparers.equal_to(
            item1.schema,
            item2.schema)
        &&
        FlatBuffersSchemas::ReflectionSchema_TypeComparers.equal_to(
            item1.type,
            item2.type)
        &&
        FlatBuffersSchemas::ReflectionSchema_ObjectComparers.equal_to(
            item1.object,
            item2.object);
}

}