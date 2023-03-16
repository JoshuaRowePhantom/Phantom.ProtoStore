#include "Schema.h"
#include "ProtoStoreInternal.pb.h"
#include "ValueComparer.h"
#include <google/protobuf/dynamic_message.h>
#include <array>
#include <flatbuffers/reflection.h>
#include "Resources.h"

namespace Phantom::ProtoStore
{

void SchemaDescriptions::AddFileToMessageDescription(
    std::set<const google::protobuf::FileDescriptor*>& addedFileDescriptors,
    google::protobuf::FileDescriptorSet* fileDescriptorSet,
    const google::protobuf::FileDescriptor* fileDescriptor)
{
    for (auto dependencyIndex = 0; dependencyIndex < fileDescriptor->dependency_count(); dependencyIndex++)
    {
        AddFileToMessageDescription(
            addedFileDescriptors,
            fileDescriptorSet,
            fileDescriptor->dependency(dependencyIndex));
    }

    auto [iterator, inserted] = addedFileDescriptors.insert(fileDescriptor);
    if (inserted)
    {
        auto fileDescriptorProto = fileDescriptorSet->add_file();
        fileDescriptor->CopyTo(fileDescriptorProto);
    }
}

shared_ptr<const Schema> SchemaDescriptions::MakeSchema(
    FlatValue<FlatBuffers::IndexSchemaDescription> indexSchemaDescription)
{
    // Construct a set of objects to hold references to as long
    // as part of the shared_ptr holding the Schema.
    struct SchemaHolder
    {
        Schema schema;
        FlatValue<FlatBuffers::IndexSchemaDescription> indexSchemaDescription;
        std::list<std::any> objects;
    };

    auto schema = std::make_shared<SchemaHolder>();
    schema->indexSchemaDescription = std::move(indexSchemaDescription);

    auto getProtocolBufferDescriptor = [&](const FlatBuffers::ProtocolBuffersSchemaDescription* description)
    {
        auto messageDescription = description->message_description();
        auto generatedDescriptorPool = google::protobuf::DescriptorPool::generated_pool();
        auto generatedDescriptor = generatedDescriptorPool->FindMessageTypeByName(
            messageDescription->message_name()->str());

        std::string fileDescriptorSetString(
            reinterpret_cast<const char*>(messageDescription->file_descriptors()->data()),
            messageDescription->file_descriptors()->size());

        google::protobuf::FileDescriptorSet fileDescriptorSet;
        fileDescriptorSet.ParseFromString(
            fileDescriptorSetString);

        auto descriptorPool = make_shared<google::protobuf::DescriptorPool>();
        for (const auto& fileDescriptorProto : fileDescriptorSet.file())
        {
            if (!descriptorPool->BuildFile(
                fileDescriptorProto))
            {
                throw std::exception("Error building descriptor");
            }
        }

        auto messageDescriptor = descriptorPool->FindMessageTypeByName(
            messageDescription->message_name()->str());
        if (!messageDescriptor)
        {
            throw std::exception("Error finding message descriptor");
        }

        // Ensure the descriptor pool last at least as long as the schema.
        schema->objects.push_back(
            descriptorPool);

        return messageDescriptor;
    };

    auto getFlatBufferObjectSchema = [&](const FlatBuffers::FlatBuffersSchemaDescription* schemaDescription)
    {
        auto schema = reinterpret_cast<const reflection::Schema*>(
            schemaDescription->object_description()->schema());

        return FlatBuffersObjectSchema
        {
            schema,
            schema->objects()->Get(schemaDescription->object_description()->object_index()),
            schemaDescription->graph_encoding_options(),
            schemaDescription->string_encoding_options(),
            schemaDescription->message_encoding_options(),
        };
    };

    if (schema->indexSchemaDescription->key()->description_as_ProtocolBuffersSchemaDescription())
    {
        ProtocolBuffersKeySchema keySchema;
        keySchema.ObjectSchema.MessageDescriptor = getProtocolBufferDescriptor(
            schema->indexSchemaDescription->key()->description_as_ProtocolBuffersSchemaDescription());

        schema->schema.KeySchema.FormatSchema = keySchema;
    }

    if (schema->indexSchemaDescription->key()->description_as_FlatBuffersSchemaDescription())
    {
        schema->schema.KeySchema.FormatSchema = FlatBuffersKeySchema
        {
            .ObjectSchema = getFlatBufferObjectSchema(
                schema->indexSchemaDescription->key()->description_as_FlatBuffersSchemaDescription()),
        };
    }

    if (schema->indexSchemaDescription->value()->description_as_ProtocolBuffersSchemaDescription())
    {
        ProtocolBuffersValueSchema valueSchema;
        valueSchema.ObjectSchema.MessageDescriptor = getProtocolBufferDescriptor(
            schema->indexSchemaDescription->value()->description_as_ProtocolBuffersSchemaDescription());

        schema->schema.ValueSchema.FormatSchema = valueSchema;
    }

    if (schema->indexSchemaDescription->value()->description_as_FlatBuffersSchemaDescription())
    {
        schema->schema.ValueSchema.FormatSchema = FlatBuffersValueSchema
        {
            .ObjectSchema = getFlatBufferObjectSchema(
                schema->indexSchemaDescription->value()->description_as_FlatBuffersSchemaDescription()),
        };
    }

    return std::shared_ptr<const Schema>(
        std::move(schema),
        &schema->schema
    );
}

shared_ptr<ValueComparer> SchemaDescriptions::MakeKeyComparer(
    std::shared_ptr<const Schema> schema)
{
    std::shared_ptr<ValueComparer> keyComparer;

    auto protocolBuffersFormatSchema = get_if<ProtocolBuffersKeySchema>(&schema->KeySchema.FormatSchema);
    auto flatbuffersFormatSchema = get_if<FlatBuffersKeySchema>(&schema->KeySchema.FormatSchema);

    if (protocolBuffersFormatSchema)
    {
        keyComparer = std::make_shared<ProtocolBuffersValueComparer>(
            protocolBuffersFormatSchema->ObjectSchema.MessageDescriptor);
    }
    else if (flatbuffersFormatSchema)
    {
        keyComparer = MakeFlatBufferValueComparer(
            std::shared_ptr<const FlatBuffersObjectSchema>
            {
                schema,
                &flatbuffersFormatSchema->ObjectSchema,
            });
    }
    else
    {
        throw std::range_error("messageDescription");
    }

    struct holder
    {
        shared_ptr<const Schema> schema;
        shared_ptr<ValueComparer> keyComparer;
    };

    auto holderPointer = std::make_shared<holder>(
        schema,
        keyComparer);

    return shared_ptr<ValueComparer>(
        holderPointer,
        holderPointer->keyComparer.get()
    );
}

shared_ptr<ValueComparer> SchemaDescriptions::MakeValueComparer(
    std::shared_ptr<const Schema> schema)
{
    std::shared_ptr<ValueComparer> keyComparer;

    if (holds_alternative<ProtocolBuffersValueSchema>(schema->ValueSchema.FormatSchema))
    {
        keyComparer = std::make_shared<ProtocolBuffersValueComparer>(
            get<ProtocolBuffersValueSchema>(
                schema->ValueSchema.FormatSchema
            ).ObjectSchema.MessageDescriptor);
    }
    else if (holds_alternative<FlatBuffersValueSchema>(schema->ValueSchema.FormatSchema))
    {
        keyComparer = MakeFlatBufferValueComparer(
            std::shared_ptr<const FlatBuffersObjectSchema>
        {
            schema,
            &get<FlatBuffersValueSchema>(schema->ValueSchema.FormatSchema).ObjectSchema
        });
    }
    else
    {
        throw std::range_error("messageDescription");
    }

    struct holder
    {
        shared_ptr<const Schema> schema;
        shared_ptr<ValueComparer> keyComparer;
    };

    auto holderPointer = std::make_shared<holder>(
        schema,
        keyComparer);

    return shared_ptr<ValueComparer>(
        holderPointer,
        holderPointer->keyComparer.get()
    );
}

ProtoValue SchemaDescriptions::MakeProtoValueKey(
    const Schema& schema,
    const FlatBuffers::DataValue* value,
    const FlatBuffers::ValuePlaceholder* valuePlaceholder
)
{
    return MakeProtoValueKey(
        schema,
        AlignedMessageData
        {
            nullptr,
            GetAlignedMessage(value),
        },
        valuePlaceholder);
}

ProtoValue SchemaDescriptions::MakeProtoValueKey(
    const Schema& schema,
    AlignedMessageData value,
    const FlatBuffers::ValuePlaceholder* placeholder
)
{
    if (schema.KeySchema.IsProtocolBuffersSchema())
    {
        return ProtoValue::ProtocolBuffer(
            value);
    }
    else
    {
        assert(schema.KeySchema.IsFlatBuffersSchema());

        auto result = ProtoValue::FlatBuffer(
            value);
        if (placeholder)
        {
            return std::move(result).SubValue(reinterpret_cast<const flatbuffers::Table*>(placeholder));
        }
        return std::move(result);
    }
}

ProtoValue SchemaDescriptions::MakeProtoValueValue(
    const Schema& schema,
    AlignedMessageData value
)
{
    if (schema.ValueSchema.IsProtocolBuffersSchema())
    {
        return ProtoValue::ProtocolBuffer(
            value);
    }
    else
    {
        assert(schema.ValueSchema.IsFlatBuffersSchema());

        return ProtoValue::FlatBuffer(
            value);
    }
}

ProtoValueComparers SchemaDescriptions::MakeKeyComparers(
    std::shared_ptr<const Schema> schema
)
{
    auto keyComparer = MakeKeyComparer(
        schema);

    return ProtoValueComparers
    {
        .comparer = [=](auto& value1, auto& value2)
    {
        return keyComparer->Compare(value1, value2);
    },
        .equal_to = [=](auto& value1, auto& value2)
    {
        return keyComparer->Compare(value1, value2) == std::weak_ordering::equivalent;
    },
        .hash = [=](auto& value1)
    {
        return keyComparer->Hash(value1);
    },
        .less = [=](auto& value1, auto& value2)
    {
        return keyComparer->Compare(value1, value2) == std::weak_ordering::less;
    },
    };
}

ProtoValueComparers ProtocolBuffersObjectSchema::MakeComparers() const
{
    auto schema = std::make_shared<Phantom::ProtoStore::Schema>(
        Schema::Make(
            KeySchema{ ProtocolBuffersKeySchema{.ObjectSchema = *this, } },
            ValueSchema {}
    ));

    return SchemaDescriptions::MakeKeyComparers(
        std::move(schema)
    );
}

ProtoValueComparers FlatBuffersObjectSchema::MakeComparers() const
{
    auto schema = std::make_shared<Phantom::ProtoStore::Schema>(
        Schema::Make(
            KeySchema{ FlatBuffersKeySchema { .ObjectSchema = *this, } },
            ValueSchema{}
    ));

    return SchemaDescriptions::MakeKeyComparers(
        std::move(schema)
    );
}

flatbuffers::Offset<FlatBuffers::ProtocolBuffersMessageDescription> SchemaDescriptions::CreateProtocolBuffersObjectDescription(
    flatbuffers::FlatBufferBuilder& builder,
    const ProtocolBuffersObjectSchema& objectSchema
)
{
    google::protobuf::FileDescriptorSet fileDescriptorSet;

    auto messageNameOffset = builder.CreateString(
        objectSchema.MessageDescriptor->full_name()
    );

    std::set<const google::protobuf::FileDescriptor*> addedFileDescriptors;

    AddFileToMessageDescription(
        addedFileDescriptors,
        &fileDescriptorSet,
        objectSchema.MessageDescriptor->file()
    );

    auto fileDescriptorSetString = fileDescriptorSet.SerializeAsString();

    auto fileDescriptorSetOffset = builder.CreateVector(
        reinterpret_cast<const uint8_t*>(fileDescriptorSetString.data()),
        fileDescriptorSetString.size()
    );

    return FlatBuffers::CreateProtocolBuffersMessageDescription(
        builder,
        fileDescriptorSetOffset,
        messageNameOffset
    );
}

flatbuffers::Offset<FlatBuffers::FlatBuffersObjectDescription> SchemaDescriptions::CreateFlatBuffersObjectDescription(
    flatbuffers::FlatBufferBuilder& builder,
    const FlatBuffersObjectSchema& objectSchema
)
{
    auto keySchema = objectSchema.Schema;

    auto keySchemaOffset = flatbuffers::CopyTable(
        builder,
        *FlatBuffersSchemas::ReflectionSchema,
        *FlatBuffersSchemas::ReflectionSchema_Schema,
        *reinterpret_cast<const flatbuffers::Table*>(keySchema)
    );

    return FlatBuffers::CreateFlatBuffersObjectDescription(
        builder,
        { keySchemaOffset.o },
        FindObjectIndex(keySchema, objectSchema.Object));
}

flatbuffers::Offset<FlatBuffers::IndexSchemaDescription> SchemaDescriptions::CreateSchemaDescription(
    flatbuffers::FlatBufferBuilder& builder,
    const Schema& schema
)
{
    using flatbuffers::Offset;
    
    FlatBuffers::SchemaDescription keySchemaDescriptionType = FlatBuffers::SchemaDescription::NONE;
    Offset<void> keySchemaDescriptionTypeOffset;

    auto flatbuffersKeySchema = schema.KeySchema.AsFlatBuffersKeySchema();
    if (flatbuffersKeySchema)
    {
        keySchemaDescriptionType = FlatBuffers::SchemaDescription::FlatBuffersSchemaDescription;
        auto flatBuffersObjectDescriptionOffset = CreateFlatBuffersObjectDescription(
            builder,
            flatbuffersKeySchema->ObjectSchema
        );

        keySchemaDescriptionTypeOffset = FlatBuffers::CreateFlatBuffersSchemaDescription(
            builder,
            flatBuffersObjectDescriptionOffset,
            flatbuffersKeySchema->ObjectSchema.GraphEncodingOptions,
            flatbuffersKeySchema->ObjectSchema.StringEncodingOptions,
            flatbuffersKeySchema->ObjectSchema.MessageEncodingOptions
        ).Union();
    }

    auto protocolBuffersKeySchema = schema.KeySchema.AsProtocolBuffersKeySchema();
    if (protocolBuffersKeySchema)
    {
        keySchemaDescriptionType = FlatBuffers::SchemaDescription::ProtocolBuffersSchemaDescription;

        auto protocolBuffersObjectDescriptionOffset = CreateProtocolBuffersObjectDescription(
            builder,
            protocolBuffersKeySchema->ObjectSchema
        );

        keySchemaDescriptionTypeOffset = FlatBuffers::CreateProtocolBuffersSchemaDescription(
            builder,
            protocolBuffersObjectDescriptionOffset
        ).Union();
    }

    if (keySchemaDescriptionType == FlatBuffers::SchemaDescription::NONE)
    {
        throw std::runtime_error("Invalid key schema");
    }

    auto keySchemaDescriptionOffset = FlatBuffers::CreateKeySchemaDescription(
        builder,
        keySchemaDescriptionType,
        keySchemaDescriptionTypeOffset
    );

    FlatBuffers::SchemaDescription valueSchemaDescriptionType = FlatBuffers::SchemaDescription::NONE;
    Offset<void> valueSchemaDescriptionTypeOffset;

    auto flatbuffersValueSchema = schema.ValueSchema.AsFlatBuffersValueSchema();
    if (flatbuffersValueSchema)
    {
        valueSchemaDescriptionType = FlatBuffers::SchemaDescription::FlatBuffersSchemaDescription;
        auto flatBuffersObjectDescriptionOffset = CreateFlatBuffersObjectDescription(
            builder,
            flatbuffersValueSchema->ObjectSchema
        );

        valueSchemaDescriptionTypeOffset = FlatBuffers::CreateFlatBuffersSchemaDescription(
            builder,
            flatBuffersObjectDescriptionOffset,
            flatbuffersValueSchema->ObjectSchema.GraphEncodingOptions,
            flatbuffersValueSchema->ObjectSchema.StringEncodingOptions,
            flatbuffersValueSchema->ObjectSchema.MessageEncodingOptions
        ).Union();
    }

    auto protocolBuffersValueSchema = schema.ValueSchema.AsProtocolBuffersValueSchema();
    if (protocolBuffersValueSchema)
    {
        valueSchemaDescriptionType = FlatBuffers::SchemaDescription::ProtocolBuffersSchemaDescription;
        auto protocolBuffersObjectDescriptionOffset = CreateProtocolBuffersObjectDescription(
            builder,
            protocolBuffersValueSchema->ObjectSchema
        );
        
        valueSchemaDescriptionTypeOffset = FlatBuffers::CreateProtocolBuffersSchemaDescription(
            builder,
            protocolBuffersObjectDescriptionOffset
        ).Union();
    }

    if (valueSchemaDescriptionType == FlatBuffers::SchemaDescription::NONE)
    {
        throw std::runtime_error("Invalid value schema");
    }

    auto valueSchemaDescriptionOffset = FlatBuffers::CreateValueSchemaDescription(
        builder,
        valueSchemaDescriptionType,
        valueSchemaDescriptionTypeOffset
    );

    auto schemaOffset = FlatBuffers::CreateIndexSchemaDescription(
        builder,
        keySchemaDescriptionOffset,
        valueSchemaDescriptionOffset
    );

    return schemaOffset;
}

uint32_t SchemaDescriptions::FindObjectIndex(
    const reflection::Schema* schema,
    const reflection::Object* object
)
{
    for (auto index = 0; index < schema->objects()->size(); ++index)
    {
        if (object == schema->objects()->Get(index))
        {
            return index;
        }
    }

    throw std::range_error("object is not in schema");
}


}