#include "Schema.h"
#include "ProtoStoreInternal.pb.h"
#include "KeyComparer.h"
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

void SchemaDescriptions::MakeSchemaDescription(
    Serialization::SchemaDescription& schemaDescription,
    const Descriptor* messageDescriptor
)
{
    schemaDescription.Clear();
    auto messageDescription = schemaDescription.mutable_protocolbuffersdescription()->mutable_messagedescription();
    messageDescription->set_messagename(
        messageDescriptor->full_name());
    
    std::set<const google::protobuf::FileDescriptor*> addedFileDescriptors;

    AddFileToMessageDescription(
        addedFileDescriptors,
        messageDescription->mutable_filedescriptors(),
        messageDescriptor->file()
    );
}

void SchemaDescriptions::MakeSchemaDescription(
    Serialization::SchemaDescription& schemaDescription,
    const FlatBuffersObjectSchema& objectSchema
)
{
    schemaDescription.Clear();
    auto objectDescription = schemaDescription.mutable_flatbuffersdescription()->mutable_objectdescription();

    flatbuffers::FlatBufferBuilder schemaBytesBuilder;
    auto rootOffset = flatbuffers::CopyTable(
        schemaBytesBuilder,
        *FlatBuffersSchemas::ReflectionSchema,
        *FlatBuffersSchemas::ReflectionSchema_Schema,
        *reinterpret_cast<const flatbuffers::Table*>(objectSchema.Schema),
        true
    );
    schemaBytesBuilder.Finish(rootOffset);

    auto bufferSpan = schemaBytesBuilder.GetBufferSpan();

    objectDescription->set_schemabytes(
        reinterpret_cast<const char*>(bufferSpan.data()),
        bufferSpan.size());

    bool foundObject = false;
    for (auto index = 0; index < objectSchema.Schema->objects()->size(); index++)
    {
        if (objectSchema.Schema->objects()->Get(index) == objectSchema.Object)
        {
            objectDescription->set_objectindex(
                index
            );
            foundObject = true;
        }
    }

    if (!foundObject)
    {
        throw std::range_error("objectSchema.Object is not in objectSchema.Schema");
    }
}

void SchemaDescriptions::MakeSchemaDescription(
    Serialization::IndexSchemaDescription& schemaDescription,
    const Schema& schema
)
{
    visit(
        [&](const auto& format)
        {
            MakeSchemaDescription(schemaDescription, format);
        },
        schema.KeySchema.FormatSchema);
    
    visit(
        [&](const auto& format)
        {
            MakeSchemaDescription(schemaDescription, format);
        },
        schema.ValueSchema.FormatSchema);
}

class ProtoStoreMessageFactory
    : public IMessageFactory
{
    std::any m_extraData;
    const Message* m_prototype;
    const Descriptor* m_descriptor;

public:
    ProtoStoreMessageFactory(
        const Message* protoType,
        const Descriptor* descriptor,
        std::any extraData
        );
    
    virtual const Descriptor* GetDescriptor(
    ) const override;

    virtual const Message* GetPrototype(
    ) const override;
};

ProtoStoreMessageFactory::ProtoStoreMessageFactory(
    const Message* protoType,
    const Descriptor* descriptor,
    std::any extraData
)
:
    m_extraData(extraData),
    m_prototype(protoType),
    m_descriptor(descriptor)
{}

const Descriptor* ProtoStoreMessageFactory::GetDescriptor(
) const
{
    return m_descriptor;
}


const Message* ProtoStoreMessageFactory::GetPrototype(
) const
{
    return m_prototype;
}

shared_ptr<const Schema> SchemaDescriptions::MakeSchema(
    Serialization::IndexSchemaDescription indexSchemaDescription)
{
    // Construct a set of objects to hold references to as long
    // as part of the shared_ptr holding the Schema.
    struct SchemaHolder
    {
        Schema schema;
        Serialization::IndexSchemaDescription description;
        std::list<std::any> objects;
    };

    auto schema = std::make_shared<SchemaHolder>();
    schema->description = std::move(indexSchemaDescription);

    auto getProtocolBufferDescriptor = [&](const Serialization::ProtocolBuffersSchemaDescription& description)
    {
        auto& messageDescription = description.messagedescription();
        auto generatedDescriptorPool = google::protobuf::DescriptorPool::generated_pool();
        auto generatedDescriptor = generatedDescriptorPool->FindMessageTypeByName(
            messageDescription.messagename());

        auto descriptorPool = make_shared<google::protobuf::DescriptorPool>();

        for (const auto& fileDescriptorProto : messageDescription.filedescriptors().file())
        {
            if (!descriptorPool->BuildFile(
                fileDescriptorProto))
            {
                throw std::exception("Error building descriptor");
            }
        }

        auto messageDescriptor = descriptorPool->FindMessageTypeByName(
            messageDescription.messagename());
        if (!messageDescriptor)
        {
            throw std::exception("Error finding message descriptor");
        }

        // Ensure the descriptor pool last at least as long as the schema.
        schema->objects.push_back(
            descriptorPool);

        return messageDescriptor;
    };

    auto getFlatBufferObjectSchema = [&](const Serialization::FlatBuffersObjectDescription& description)
    {
        auto schema = flatbuffers::GetRoot<reflection::Schema>(
            description.schemabytes().data());
        auto object = schema->objects()->Get(
            description.objectindex());

        return FlatBuffersObjectSchema
        {
            .Schema = schema,
            .Object = object,
        };
    };

    if (schema->description.key().description().has_protocolbuffersdescription())
    {
        ProtocolBuffersKeySchema keySchema;
        keySchema.ObjectSchema.MessageDescriptor = getProtocolBufferDescriptor(
            schema->description.key().description().protocolbuffersdescription());

        schema->schema.KeySchema.FormatSchema = keySchema;
    }

    if (schema->description.key().description().has_flatbuffersdescription())
    {
        schema->schema.KeySchema.FormatSchema = FlatBuffersKeySchema
        {
            .ObjectSchema = getFlatBufferObjectSchema(
                schema->description.key().description().flatbuffersdescription().objectdescription()),
        };
    }

    if (schema->description.value().description().has_protocolbuffersdescription())
    {
        ProtocolBuffersValueSchema valueSchema;
        valueSchema.ObjectSchema.MessageDescriptor = getProtocolBufferDescriptor(
            schema->description.value().description().protocolbuffersdescription());

        schema->schema.ValueSchema.FormatSchema = valueSchema;
    }

    if (schema->description.value().description().has_flatbuffersdescription())
    {
        schema->schema.ValueSchema.FormatSchema = FlatBuffersValueSchema
        {
            .ObjectSchema = getFlatBufferObjectSchema(
                schema->description.key().description().flatbuffersdescription().objectdescription()),
        };
    }

    return std::shared_ptr<const Schema>(
        std::move(schema),
        &schema->schema
    );
}

shared_ptr<KeyComparer> SchemaDescriptions::MakeKeyComparer(
    std::shared_ptr<const Schema> schema)
{
    std::shared_ptr<KeyComparer> keyComparer;

    if (holds_alternative<ProtocolBuffersKeySchema>(schema->KeySchema.FormatSchema))
    {
        keyComparer = std::make_shared<ProtoKeyComparer>(
            get<ProtocolBuffersKeySchema>(
                schema->KeySchema.FormatSchema
            ).ObjectSchema.MessageDescriptor);
    }
    else if (holds_alternative<FlatBuffersKeySchema>(schema->KeySchema.FormatSchema))
    {
        keyComparer = MakeFlatBufferKeyComparer(
            get<FlatBuffersKeySchema>(schema->KeySchema.FormatSchema).ObjectSchema.Schema,
            get<FlatBuffersKeySchema>(schema->KeySchema.FormatSchema).ObjectSchema.Object);
    }
    else
    {
        throw std::range_error("messageDescription");
    }

    struct holder
    {
        shared_ptr<const Schema> schema;
        shared_ptr<KeyComparer> keyComparer;
    };

    auto holderPointer = std::make_shared<holder>(
        schema,
        keyComparer);

    return shared_ptr<KeyComparer>(
        holderPointer,
        holderPointer->keyComparer.get()
    );
}

void SchemaDescriptions::MakeSchemaDescription(
    Serialization::IndexSchemaDescription& schemaDescription,
    std::monostate
)
{
    throw std::range_error("Invalid schema");
}

void SchemaDescriptions::MakeSchemaDescription(
    Serialization::IndexSchemaDescription& schemaDescription,
    const FlatBuffersKeySchema& schema
)
{
    MakeSchemaDescription(
        *schemaDescription.mutable_key()->mutable_description(),
        schema.ObjectSchema
    );
}

void SchemaDescriptions::MakeSchemaDescription(
    Serialization::IndexSchemaDescription& schemaDescription,
    const ProtocolBuffersKeySchema& schema
)
{
    MakeSchemaDescription(
        *schemaDescription.mutable_key()->mutable_description(),
        schema.ObjectSchema.MessageDescriptor
    );
}

void SchemaDescriptions::MakeSchemaDescription(
    Serialization::IndexSchemaDescription& schemaDescription,
    const FlatBuffersValueSchema& schema
)
{
    MakeSchemaDescription(
        *schemaDescription.mutable_value()->mutable_description(),
        schema.ObjectSchema
    );
}

void SchemaDescriptions::MakeSchemaDescription(
    Serialization::IndexSchemaDescription& schemaDescription,
    const ProtocolBuffersValueSchema& schema
)
{
    MakeSchemaDescription(
        *schemaDescription.mutable_value()->mutable_description(),
        schema.ObjectSchema.MessageDescriptor
    );
}

ProtoValue SchemaDescriptions::MakeProtoValueKey(
    const Schema& schema,
    const FlatBuffers::DataValue* value
)
{
    return MakeProtoValueKey(
        schema,
        AlignedMessageData
        {
            nullptr,
            GetAlignedMessage(value),
        });
}

ProtoValue SchemaDescriptions::MakeProtoValueKey(
    const Schema& schema,
    AlignedMessageData value
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

        return ProtoValue::FlatBuffer(
            value);
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

ProtoValueComparers SchemaDescriptions::MakeComparers(
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

    return SchemaDescriptions::MakeComparers(
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

    return SchemaDescriptions::MakeComparers(
        std::move(schema)
    );
}

}