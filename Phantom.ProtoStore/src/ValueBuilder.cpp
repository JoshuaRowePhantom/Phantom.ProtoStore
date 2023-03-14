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
    m_internedSchemaItems{ 0, SchemaItemComparer{}, SchemaItemComparer{} }
{}

const ValueBuilder::InternedSchemaItem& ValueBuilder::InternSchemaItem(
    const SchemaItem& schemaItem
)
{
    
    if (m_internedSchemaItems.contains(schemaItem))
    {
        return m_internedSchemaItems[schemaItem];
    }

    return m_internedSchemaItems[schemaItem] = MakeInternedSchemaItem(
        schemaItem);
}


const ValueBuilder::InternedSchemaItem ValueBuilder::MakeInternedSchemaItem(
    const SchemaItem& schemaItem
)
{
    throw 0;
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