#pragma once

#include "StandardTypes.h"
#include <set>

namespace Phantom::ProtoStore
{

class IMessageFactory
{
public:
    virtual const Descriptor* GetDescriptor(
    ) const = 0;

    virtual const Message* GetPrototype(
    ) const = 0;
};

class SchemaDescriptions
{
    static void AddFileToMessageDescription(
        std::set<const google::protobuf::FileDescriptor*>& addedFileDescriptors,
        google::protobuf::FileDescriptorSet* fileDescriptorSet,
        const google::protobuf::FileDescriptor* fileDescriptor);

public:
    static void MakeSchemaDescription(
        Serialization::IndexSchemaDescription& schemaDescription,
        const Schema& schema
    );

    static void MakeSchemaDescription(
        Serialization::SchemaDescription& messageDescription,
        const Descriptor* messageDescriptor
    );
    
    static void MakeSchemaDescription(
        Serialization::SchemaDescription& schemaDescription,
        const FlatBuffersObjectSchema& objectSchema
    );

    static void MakeSchemaDescription(
        Serialization::IndexSchemaDescription& schemaDescription,
        std::monostate
    );

    static void MakeSchemaDescription(
        Serialization::IndexSchemaDescription& schemaDescription,
        const FlatBuffersKeySchema& schema
    );

    static void MakeSchemaDescription(
        Serialization::IndexSchemaDescription& schemaDescription,
        const ProtocolBuffersKeySchema& schema
    );

    static void MakeSchemaDescription(
        Serialization::IndexSchemaDescription& schemaDescription,
        const FlatBuffersValueSchema& schema
    );

    static void MakeSchemaDescription(
        Serialization::IndexSchemaDescription& schemaDescription,
        const ProtocolBuffersValueSchema& schema
    );
    
    static shared_ptr<const Schema> MakeSchema(
        Serialization::IndexSchemaDescription indexSchemaDescription
    );

    static shared_ptr<KeyComparer> MakeKeyComparer(
        std::shared_ptr<const Schema> schema);

    static ProtoValue MakeProtoValueKey(
        const Schema& schema,
        const FlatBuffers::DataValue* dataValue
    );

    static ProtoValue MakeProtoValueKey(
        const Schema& schema,
        const AlignedMessage& alignedMessage
    );
};


}