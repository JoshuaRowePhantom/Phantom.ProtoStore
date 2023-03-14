#include "KeyComparer.h"
#include "Resources.h"

namespace Phantom::ProtoStore
{

// Hash computation
size_t ValueBuilder::InternedValueKeyComparer::operator()(
    const auto& key
    ) const
{
    return key.hashCode;
}

// Equality comparison
bool ValueBuilder::InternedValueKeyComparer::operator()(
    const auto& value1,
    const auto& value2
    ) const
{
    return m_valueBuilder->Equals(value1, value2);
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

    if (value1.hashCode != value2.hashCode)
    {
        return false;
    }

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
        .offset = offset,
        .hashCode = hashCode
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
    const void* value,
    size_t& hash
)
{
    auto& internedSchemaItem = InternSchemaItem(schemaItem);
    
    hash = internedSchemaItem.hash(value);

    auto internedItem = m_internedValues.find(
        UninternedValue
        {
            value,
            &internedSchemaItem,
            hash
        });

    if (internedItem == m_internedValues.end())
    {
        return {0};
    }

    return internedItem->offset;
}

void ValueBuilder::InternValue(
    const SchemaItem& schemaItem,
    flatbuffers::Offset<void> offset,
    size_t hash
)
{
    auto& internedSchemaItem = InternSchemaItem(schemaItem);

    m_internedValues.insert(
        InterningValue
        {
            &internedSchemaItem,
            offset,
            hash
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

void ValueBuilder::CopyPrimitive(
    const reflection::Field* field,
    const flatbuffers::Table* table,
    size_t align,
    size_t size
)
{
    builder().Align(align);
    builder().PushBytes(
        table->GetStruct<const uint8_t*>(
            field->offset()),
        size);
    builder().TrackField(
        field->offset(),
        builder().GetSize());
}

flatbuffers::Offset<flatbuffers::Table> ValueBuilder::CopyTableDag(
    const reflection::Schema* schema,
    const reflection::Object* object,
    const flatbuffers::Table* table
)
{
    using flatbuffers::uoffset_t;
    using flatbuffers::Offset;
    using reflection::BaseType;

    // Obviously, we should see if the table is already interned.
    size_t hash;
    auto internedTable = GetInternedValue(
        SchemaItem
        {
            schema,
            object,
            nullptr
        },
        table,
        hash);
    
    if (!internedTable.IsNull())
    {
        return flatbuffers::Offset<flatbuffers::Table>(internedTable.o);
    }

    // Copy all the sub-objects.
    std::vector<uoffset_t> offsets;

    for (auto field : *object->fields())
    {
        if (!table->CheckField(field->offset()))
        {
            continue;
        }
        
        switch (field->type()->base_type())
        {
        case BaseType::String:
        {
            offsets.push_back(
                builder().CreateSharedString(
                    GetFieldS(*table, *field)).o);
            break;
        }

        case BaseType::Obj:
        {
            auto subObject = schema->objects()->Get(field->type()->index());
            if (subObject->is_struct())
            {
                continue;
            }
            offsets.push_back(
                CopyTableDag(
                    schema,
                    subObject,
                    flatbuffers::GetFieldT(*table, *field)).o);
            break;
        }

        case BaseType::Vector:
        {
            offsets.push_back(
                CopyVectorDag(
                    schema,
                    field->type(),
                    table->GetPointer<const flatbuffers::VectorOfAny*>(field->offset())).o);
            break;
        }

        case BaseType::Union:
        {
            auto typeField = std::find_if(
                object->fields()->begin(),
                object->fields()->end(),
                [&](const ::reflection::Field* otherField)
            {
                return otherField->id() == field->id() - 1;
            });

            auto unionEnum = schema->enums()->Get(typeField->type()->index());
            auto unionType = flatbuffers::GetFieldI<uint8_t>(*table, **typeField);
            auto unionEnumValue = unionEnum->values()->LookupByKey(unionType);
            auto unionObject = schema->objects()->Get(unionEnumValue->union_type()->index());

            offsets.push_back(
                CopyTableDag(
                    schema,
                    unionObject,
                    flatbuffers::GetFieldT(*table, *field)).o);
            break;
        }
        }
    }

    // Now that we've copied the subobjects,
    // we can copy the object itself.
    
    // When we run across a subobject, pull the offset from the list
    // of already-copied offsets. This variable keeps track of how
    // far into the offsets vector we are.
    auto offsetsIndex = 0;

    auto start = builder().StartTable();

    for (auto field : *object->fields())
    {
        if (!table->CheckField(field->offset()))
        {
            continue;
        }

        auto baseType = field->type()->base_type();
        switch (baseType)
        {
        case BaseType::Obj:
        {
            auto subObject = schema->objects()->Get(field->type()->index());
            if (subObject->is_struct())
            {
                CopyPrimitive(
                    field,
                    table,
                    subObject->minalign(),
                    subObject->bytesize());
                break;
            }
        }
        // Fall through for non-struct objects.

        case BaseType::String:
        case BaseType::Union:
        case BaseType::Vector:
            builder().AddOffset(field->offset(), Offset<void>(offsets[offsetsIndex++]));
            break;

        default:
        {
            // Scalar types
            auto size = flatbuffers::GetTypeSize(baseType);
            CopyPrimitive(
                field,
                table,
                size,
                size);
        }
        }
    }

    assert(offsetsIndex == offsets.size());
    Offset<flatbuffers::Table> result = builder().EndTable(start);

    InternValue(
        SchemaItem
        {
            schema,
            object,
            nullptr
        },
        result.Union(),
        hash);

    return result;
}

flatbuffers::Offset<flatbuffers::VectorOfAny> ValueBuilder::CopyVectorDag(
    const reflection::Schema* schema,
    const reflection::Type* type,
    const flatbuffers::VectorOfAny* vector
)
{
    using flatbuffers::uoffset_t;
    using flatbuffers::Offset;
    using reflection::BaseType;

    // Obviously, we should see if the table is already interned.
    size_t hash;
    auto internedTable = GetInternedValue(
        SchemaItem
        {
            schema,
            nullptr,
            type
        },
        vector,
        hash);

    if (!internedTable.IsNull())
    {
        return flatbuffers::Offset<flatbuffers::VectorOfAny>(internedTable.o);
    }

    Offset<flatbuffers::VectorOfAny> result;

    auto elementObject =
        type->element() == BaseType::Obj
        ?
        schema->objects()->Get(type->index())
        :
        nullptr;

    switch (type->element())
    {
    case BaseType::String:
    {
        std::vector<Offset<flatbuffers::String>> offsets;
        auto stringVector = reinterpret_cast<const flatbuffers::Vector<Offset<flatbuffers::String>>*>(vector);
        offsets.reserve(vector->size());

        for (auto i = 0; i < stringVector->size(); ++i)
        {
            offsets.push_back(
                builder().CreateSharedString(
                    stringVector->Get(i)));
        }

        result = builder().CreateVector(offsets).o;
        break;
    }

    case BaseType::Obj:
    {
        if (!elementObject->is_struct())
        {
            std::vector<Offset<flatbuffers::Table>> offsets;
            auto objectVector = reinterpret_cast<const flatbuffers::Vector<Offset<flatbuffers::Table>>*>(vector);
            for (auto i = 0; i < objectVector->size(); ++i)
            {
                offsets.push_back(
                    CopyTableDag(
                        schema,
                        elementObject,
                        objectVector->Get(i)));
            }
            result = builder().CreateVector(offsets).o;
            break;
        }
    }
    // Fall through if struct

    default:
    // Scalar or structure type.
    {
        auto elementSize = flatbuffers::GetTypeSize(type->element());
        auto alignment = elementSize;
        if (elementObject)
        {
            elementSize = elementObject->bytesize();
            alignment = elementObject->minalign();
        }
        
        builder().StartVector(
            vector->size(),
            elementSize,
            alignment);

        builder().PushBytes(
            vector->Data(),
            elementSize * vector->size());

        result = builder().EndVector(vector->size());
        break;
    }
    }

    InternValue(
        SchemaItem
        {
            schema,
            nullptr,
            type
        },
        result.Union(),
        hash);
    return result;
}

}