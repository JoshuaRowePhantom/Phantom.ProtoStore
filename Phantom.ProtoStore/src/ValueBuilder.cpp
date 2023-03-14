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

ValueBuilder::ValueBuilder(
) :
    m_internedValues{ 0, InternedValueKeyComparer{ this }, InternedValueKeyComparer{ this } },
    m_internedSchemaItems{ std::make_shared<InternedSchemaItems>() }
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

const ValueBuilder::InternedSchemaItem& ValueBuilder::InternSchemaItem(
    const SchemaItem& schemaItem
)
{
    if (m_internedSchemaItemsByPointer.contains(schemaItem.schemaIdentifier()))
    {
        return *m_internedSchemaItemsByPointer[schemaItem.schemaIdentifier()];
    }

    return *(m_internedSchemaItemsByPointer[schemaItem.schemaIdentifier()] = &m_internedSchemaItems->InternSchemaItem(
        schemaItem));
}

ValueBuilder::InternedSchemaItems::InternedSchemaItems()
    :
    m_internedSchemaItemsByItem{ 0, SchemaItemComparer{}, SchemaItemComparer{} }
{}

const ValueBuilder::InternedSchemaItem& ValueBuilder::InternedSchemaItems::InternSchemaItem(
    const SchemaItem& schemaItem
)
{
    std::unique_lock lock{ m_mutex };

    if (m_internedSchemaItemsByPointer.contains(schemaItem.schemaIdentifier()))
    {
        return *m_internedSchemaItemsByPointer[schemaItem.schemaIdentifier()];
    }

    if (m_internedSchemaItemsByItem.contains(schemaItem))
    {
        return *(m_internedSchemaItemsByPointer[schemaItem.schemaIdentifier()] = m_internedSchemaItemsByItem[schemaItem].get());
    }

    return *(
        m_internedSchemaItemsByPointer[schemaItem.schemaIdentifier()] = (
            m_internedSchemaItemsByItem[schemaItem] = std::make_shared<InternedSchemaItem>(
                MakeInternedSchemaItem(
                    schemaItem))).get());
}

ValueBuilder::InternedSchemaItem ValueBuilder::InternedSchemaItems::MakeInternedSchemaItem(
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

ValueBuilder::InternedSchemaItem ValueBuilder::InternedSchemaItems::MakeInternedVectorSchemaItem(
    const SchemaItem& schemaItem
)
{
    return InternedSchemaItem
    {
        .schemaIdentifier = schemaItem.schemaIdentifier(),
        .hash = [=](const void* v)
        {
            return ValueBuilder::Hash(
                schemaItem.schema,
                schemaItem.type,
                reinterpret_cast<const flatbuffers::VectorOfAny*>(v));
        },
        .equal_to = [=](const void* a, const void* b)
        {
            return ValueBuilder::Equals(
                schemaItem.schema,
                schemaItem.type,
                reinterpret_cast<const flatbuffers::VectorOfAny*>(a),
                reinterpret_cast<const flatbuffers::VectorOfAny*>(b));
        },
    };
}

ValueBuilder::InternedSchemaItem ValueBuilder::InternedSchemaItems::MakeInternedObjectSchemaItem(
    const SchemaItem& schemaItem
)
{
    return InternedSchemaItem
    {
        .schemaIdentifier = schemaItem.schemaIdentifier(),
        .hash = [=](const void* v)
        {
            return ValueBuilder::Hash(
                schemaItem.schema,
                schemaItem.object,
                reinterpret_cast<const flatbuffers::Table*>(v));
        },
        .equal_to = [=](const void* a, const void* b)
        {
            return ValueBuilder::Equals(
                schemaItem.schema,
                schemaItem.object,
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
)
{
    return m_flatBufferBuilder;
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
size_t ValueBuilder::InternedSchemaItems::SchemaItemComparer::operator()(
    const SchemaItem& item
    ) const
{
    return
        Hash(
            FlatBuffersSchemas::ReflectionSchema,
            FlatBuffersSchemas::ReflectionSchema_Schema,
            reinterpret_cast<const flatbuffers::Table*>(item.schema))
        ^
        Hash(
            FlatBuffersSchemas::ReflectionSchema,
            FlatBuffersSchemas::ReflectionSchema_Type,
            reinterpret_cast<const flatbuffers::Table*>(item.type))
        ^
        Hash(
            FlatBuffersSchemas::ReflectionSchema,
            FlatBuffersSchemas::ReflectionSchema_Object,
            reinterpret_cast<const flatbuffers::Table*>(item.object));
}

// Equality computation
bool ValueBuilder::InternedSchemaItems::SchemaItemComparer::operator()(
    const SchemaItem& item1,
    const SchemaItem& item2
    ) const
{
    return
        Equals(
            FlatBuffersSchemas::ReflectionSchema,
            FlatBuffersSchemas::ReflectionSchema_Schema,
            reinterpret_cast<const flatbuffers::Table*>(item1.schema),
            reinterpret_cast<const flatbuffers::Table*>(item2.schema))
        &&
        Equals(
            FlatBuffersSchemas::ReflectionSchema,
            FlatBuffersSchemas::ReflectionSchema_Type,
            reinterpret_cast<const flatbuffers::Table*>(item1.type),
            reinterpret_cast<const flatbuffers::Table*>(item2.type))
        &&
        Equals(
            FlatBuffersSchemas::ReflectionSchema,
            FlatBuffersSchemas::ReflectionSchema_Object,
            reinterpret_cast<const flatbuffers::Table*>(item1.object),
            reinterpret_cast<const flatbuffers::Table*>(item2.object));
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


void ValueBuilder::AddSchema(
    const reflection::Schema* schema)
{
    for (auto object : *schema->objects())
    {
        if (object->is_struct())
        {
            continue;
        }

        InternSchemaItem(
            SchemaItem
            {
                schema,
                object,
                nullptr
            });

        for (auto field : *object->fields())
        {
            if (field->type()->base_type() == reflection::BaseType::Vector)
            {
                InternSchemaItem(
                    SchemaItem
                    {
                        schema,
                        nullptr,
                        field->type()
                    });
            }
        }
    }
}

ValueBuilder::ValueBuilder(
    const ValueBuilder& other
) :
    m_internedSchemaItems{ other.m_internedSchemaItems },
    m_internedSchemaItemsByPointer { other.m_internedSchemaItemsByPointer }
{
}

ValueBuilder ValueBuilder::CreateNew() const
{
    return ValueBuilder(*this);
}

void ValueBuilder::Clear()
{
    m_internedValues.clear();
    m_flatBufferBuilder.Clear();
}

size_t ValueBuilder::Hash(
    const reflection::Schema* schema,
    const reflection::Object* object,
    const flatbuffers::Table* table)
{
    hash_v1_type hash;
    Hash(
        hash,
        schema,
        object,
        table);
    return hash.checksum();
}

size_t ValueBuilder::Hash(
    const reflection::Schema* schema,
    const reflection::Type* type,
    const flatbuffers::VectorOfAny* vector)
{
    hash_v1_type hash;
    Hash(
        hash,
        schema,
        type,
        vector);
    return hash.checksum();
}

static std::vector<const reflection::Field*> GetSortedFields(
    const reflection::Object* object
)
{
    std::vector<const reflection::Field*> fields(object->fields()->size());
    for (auto field : *object->fields())
    {
        fields[field->id()] = field;
    }
    return fields;
}

void ValueBuilder::Hash(
    hash_v1_type& hash,
    const reflection::Schema* schema,
    const reflection::Object* object,
    const flatbuffers::Table* table)
{
    using flatbuffers::uoffset_t;
    using flatbuffers::Offset;
    using reflection::BaseType;

    static int32_t zero = 0;

    if (!table)
    {
        hash.process_bytes(&zero, sizeof(zero));
        return;
    }

    // When hashing, process fields in ID order for stability purposes.
    // This way, two different schemas that happen to have the same
    // fields in different order will hash to the same value.
    auto sortedFields = GetSortedFields(
        object);

    // When we run across a subobject, pull the offset from the list
    // of already-copied offsets. This variable keeps track of how
    // far into the offsets vector we are.
    for(auto field : sortedFields)
    {
        if (!table->CheckField(field->offset()))
        {
            hash.process_bytes(&zero, sizeof(zero));
            continue;
        }

        auto baseType = field->type()->base_type();
        switch (baseType)
        {
        case BaseType::Obj:
        {
            const reflection::Object* subObject = schema->objects()->Get(field->type()->index());
            if (subObject->is_struct())
            {
                auto subStruct = flatbuffers::GetFieldStruct(*table, *field);
                hash.process_bytes(
                    subStruct,
                    subObject->bytesize());
                break;
            }
            
            Hash(
                hash,
                schema,
                subObject,
                flatbuffers::GetFieldT(*table, *field)
            );
            break;
        }

        case BaseType::String:
        {
            auto stringView = flatbuffers::GetStringView(
                flatbuffers::GetFieldS(*table, *field)
            );
            uint32_t size = stringView.size();
            hash.process_bytes(&size, sizeof(size));
            hash.process_bytes(stringView.data(), size);
            break;
        }

        case BaseType::Union:
        {
            auto typeField = sortedFields[field->id() - 1];
            auto unionEnum = schema->enums()->Get(typeField->type()->index());
            auto unionType = flatbuffers::GetFieldI<uint8_t>(*table, *typeField);
            auto unionEnumValue = unionEnum->values()->LookupByKey(unionType);
            auto unionObject = schema->objects()->Get(unionEnumValue->union_type()->index());

            Hash(
                hash,
                schema,
                unionObject,
                flatbuffers::GetFieldT(*table, *field)
            );
            break;

        }

        case BaseType::Vector:
        {
            auto vector = flatbuffers::GetFieldAnyV(*table, *field);

            Hash(
                hash,
                schema,
                field->type(),
                vector);
            break;
        }

        default:
        {
            // Scalar types
            hash.process_bytes(
                flatbuffers::GetAnyFieldAddressOf<const void>(*table, *field),
                flatbuffers::GetTypeSize(baseType));
        }
        }
    }
}

void ValueBuilder::Hash(
    hash_v1_type& hash,
    const reflection::Schema* schema,
    const reflection::Type* type,
    const flatbuffers::VectorOfAny* vector
)
{
    using reflection::BaseType;

    static int32_t zero = 0;

    if (!vector)
    {
        hash.process_bytes(&zero, sizeof(zero));
    }

    auto elementObject =
        type->element() == BaseType::Obj
        ?
        schema->objects()->Get(type->index())
        :
        nullptr;

    auto vectorSize = vector->size();
    hash.process_bytes(&vectorSize, sizeof(vectorSize));

    switch (type->element())
    {
    case BaseType::String:
    {
        auto stringVector = reinterpret_cast<const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>*>(vector);
        for (auto i = 0; i < vectorSize; ++i)
        {
            auto stringView = flatbuffers::GetStringView(stringVector->Get(i));
            uint32_t size = stringView.size();
            hash.process_bytes(&size, sizeof(size));
            hash.process_bytes(stringView.data(), size);
        }
        break;
    }

    case BaseType::Obj:
    {
        if (elementObject->is_struct())
        {
            goto TrivialVectorType;
        }

        auto objectVector = reinterpret_cast<const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::Table>>*>(vector);
        for (auto i = 0; i < vectorSize; ++i)
        {
            Hash(
                hash,
                schema,
                elementObject,
                objectVector->Get(i));
        }
        break;
    }

    default:
    TrivialVectorType:
    {
        auto elementSize = flatbuffers::GetTypeSize(type->element());
        if (elementObject)
        {
            elementSize = elementObject->bytesize();
        }
        hash.process_bytes(
            vector->Data(),
            elementSize * vectorSize);
        break;
    }
    }
}

bool ValueBuilder::Equals(
    const reflection::Schema* schema,
    const reflection::Object* object,
    const flatbuffers::Table* table1,
    const flatbuffers::Table* table2)
{
    using flatbuffers::Offset;
    using reflection::BaseType;

    if (table1 == table2)
    {
        return true;
    }
    if (!table1 || !table2)
    {
        return false;
    }

    auto sortedFields = GetSortedFields(
        object);

    for (auto field : sortedFields)
    {
        bool table1HasField = table1->CheckField(field->offset());
        bool table2HasField = table2->CheckField(field->offset());

        if (table1HasField != table2HasField)
        {
            return false;
        }
        if (!table1HasField)
        {
            continue;
        }

        auto baseType = field->type()->base_type();
        switch (baseType)
        {
        case BaseType::Obj:
        {
            const reflection::Object* subObject = schema->objects()->Get(field->type()->index());
            if (subObject->is_struct())
            {
                auto subStruct1 = flatbuffers::GetFieldStruct(*table1, *field);
                auto subStruct2 = flatbuffers::GetFieldStruct(*table2, *field);
                if (memcmp(subStruct1, subStruct2, subObject->bytesize()) != 0)
                {
                    return false;
                }
                break;
            }

            if (!Equals(
                schema,
                subObject,
                flatbuffers::GetFieldT(*table1, *field),
                flatbuffers::GetFieldT(*table2, *field)))
            {
                return false;
            }
            break;
        }

        case BaseType::String:
        {
            auto stringView1 = flatbuffers::GetStringView(
                flatbuffers::GetFieldS(*table1, *field)
            );
            auto stringView2 = flatbuffers::GetStringView(
                flatbuffers::GetFieldS(*table2, *field)
            );
            if (stringView1 != stringView2)
            {
                return false;
            }
            break;
        }

        case BaseType::Union:
        {
            // We already know the types are equal, because we compared the types before
            // we compare the value.

            auto typeField = sortedFields[field->id() - 1];
            auto unionEnum = schema->enums()->Get(typeField->type()->index());
            auto unionType = flatbuffers::GetFieldI<uint8_t>(*table1, *typeField);
            auto unionEnumValue = unionEnum->values()->LookupByKey(unionType);
            auto unionObject = schema->objects()->Get(unionEnumValue->union_type()->index());

            if (!Equals(
                schema,
                unionObject,
                flatbuffers::GetFieldT(*table1, *field),
                flatbuffers::GetFieldT(*table2, *field)))
            {
                return false;
            }
            break;

        }


        case BaseType::Vector:
        {
            auto vector1 = flatbuffers::GetFieldAnyV(*table1, *field);
            auto vector2 = flatbuffers::GetFieldAnyV(*table2, *field);

            if (!Equals(
                schema,
                field->type(),
                vector1,
                vector2))
            {
                return false;
            }
            break;
        }

        default:
        {
            // Scalar types
            if (memcmp(
                flatbuffers::GetAnyFieldAddressOf<const void>(*table1, *field),
                flatbuffers::GetAnyFieldAddressOf<const void>(*table2, *field),
                flatbuffers::GetTypeSize(baseType)) != 0)
            {
                return false;
            }
        }
        }
    }

    return true;
}

bool ValueBuilder::Equals(
    const reflection::Schema* schema,
    const reflection::Type* type,
    const flatbuffers::VectorOfAny* vector1,
    const flatbuffers::VectorOfAny* vector2)
{
    using reflection::BaseType;

    if (vector1 == vector2)
    {
        return true;
    }
    if (!vector1 || !vector2)
    {
        return false;
    }

    auto vectorSize = vector1->size();
    if (vectorSize != vector2->size())
    {
        return false;
    }

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
        auto stringVector1 = reinterpret_cast<const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>*>(vector1);
        auto stringVector2 = reinterpret_cast<const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>*>(vector2);
        for (auto i = 0; i < vectorSize; ++i)
        {
            auto stringView1 = flatbuffers::GetStringView(stringVector1->Get(i));
            auto stringView2 = flatbuffers::GetStringView(stringVector2->Get(i));
            if (stringView1 != stringView2)
            {
                return false;
            }
        }
        break;
    }

    case BaseType::Obj:
    {
        if (elementObject->is_struct())
        {
            goto TrivialVectorType;
        }

        auto objectVector1 = reinterpret_cast<const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::Table>>*>(vector1);
        auto objectVector2 = reinterpret_cast<const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::Table>>*>(vector2);
        for (auto i = 0; i < vectorSize; ++i)
        {
            if (!Equals(
                schema,
                elementObject,
                objectVector1->Get(i),
                objectVector2->Get(i)))
            {
                return false;
            }
        }
        break;
    }

    default:
    TrivialVectorType:
    {
        auto elementSize = flatbuffers::GetTypeSize(type->element());
        if (elementObject)
        {
            elementSize = elementObject->bytesize();
        }
        if (memcmp(vector1->Data(), vector2->Data(), elementSize * vectorSize) != 0)
        {
            return false;
        }
        break;
    }
    }

    return true;
}


}