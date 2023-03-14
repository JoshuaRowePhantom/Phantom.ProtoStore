#include "KeyComparer.h"
#include "Resources.h"

namespace Phantom::ProtoStore
{

// Hash computation
size_t ValueBuilder::InternedValueKeyComparer::operator()(
    const InternedValueKey& key
    ) const
{
    return m_valueBuilder->Hash(key);
}

// Equality comparison
bool ValueBuilder::InternedValueKeyComparer::operator()(
    const InternedValueKey& key1,
    const InternedValueKey& key2
    ) const
{
    return m_valueBuilder->Equals(key1, key2);
}

size_t ValueBuilder::Hash(
    const InternedValueKey& key
)
{
    return 0;
}

bool ValueBuilder::Equals(
    const InternedValueKey& key1,
    const InternedValueKey& key2
)
{
    return false;
}

ValueBuilder::ValueBuilder(
    flatbuffers::FlatBufferBuilder* flatBufferBuilder
) :
    m_flatBufferBuilder{ flatBufferBuilder },
    m_internedValues{ 0, InternedValueKeyComparer{ this }, InternedValueKeyComparer{ this } },
    m_internedSchemaItemsByItem{ 0, SchemaItemComparer{}, SchemaItemComparer{} }
{}

const ValueBuilder::InternedSchemaItem& ValueBuilder::InternSchemaItem(
    const SchemaItem& schemaItem
)
{
    if (m_internedSchemaItemsByPointer.contains(schemaItem.type))
    {
        return *m_internedSchemaItemsByPointer[schemaItem.type];
    }

    if (m_internedSchemaItemsByItem.contains(schemaItem))
    {
        return *(m_internedSchemaItemsByPointer[schemaItem.type] = &m_internedSchemaItemsByItem[schemaItem]);
    }

    return *(
        m_internedSchemaItemsByPointer[schemaItem.type] = &(
            m_internedSchemaItemsByItem[schemaItem] = MakeInternedSchemaItem(
                schemaItem)));
}


ValueBuilder::InternedSchemaItem ValueBuilder::MakeInternedSchemaItem(
    const SchemaItem& schemaItem
)
{
    using reflection::BaseType;

    switch (schemaItem.type->base_type())
    {
    case BaseType::String:
        return MakeInternedStringSchemaItem(schemaItem);
        
    case BaseType::Vector:
        return MakeInternedVectorSchemaItem(schemaItem);

    case BaseType::Obj:
        assert(!schemaItem.schema->objects()->Get(schemaItem.type->index())->is_struct());
        return MakeInternedObjectSchemaItem(schemaItem);

    default:
        assert(false);
        throw std::range_error("Unsupported type");
    }
}

ValueBuilder::InternedSchemaItem ValueBuilder::MakeInternedStringSchemaItem(
    const SchemaItem& schemaItem
)
{
    return InternedSchemaItem
    {
        .schemaIdentifier = schemaItem.type,
        .hash = [](const void* v)
        {
            return std::hash<std::string_view>{}(
                flatbuffers::GetStringView(
                    reinterpret_cast<const flatbuffers::String*>(v)));
        },
        .equal_to = [](const void* a, const void* b)
        {
            return
                flatbuffers::GetStringView(
                    reinterpret_cast<const flatbuffers::String*>(a))
                ==
                flatbuffers::GetStringView(
                    reinterpret_cast<const flatbuffers::String*>(b));
        },
    };
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
            .schemaIdentifier = schemaItem.type,
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
        auto objectType = schemaItem.schema->objects()->Get(schemaItem.type->index());
        if (objectType->is_struct())
        {
            goto TrivialType;
        }

        auto objectSchemaItem = MakeInternedObjectSchemaItem(
            schemaItem
        );

        return InternedSchemaItem
        {
            .schemaIdentifier = schemaItem.type,
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
            .schemaIdentifier = schemaItem.type,
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
    auto object = schemaItem.schema->objects()->Get(schemaItem.type->index());
    auto comparers =
        FlatBuffersObjectSchema
        {
            schemaItem.schema,
            object
        }.MakeComparers();

    return InternedSchemaItem
    {
        .schemaIdentifier = schemaItem.type,
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
    return {};
}

void ValueBuilder::InternValue(
    const SchemaItem& schemaItem,
    const void* value,
    flatbuffers::Offset<void> offset
)
{}

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
            item.type);
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
            item2.type);
}

}