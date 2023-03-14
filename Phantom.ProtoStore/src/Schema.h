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

    static uint32_t FindObjectIndex(
        const reflection::Schema* schema,
        const reflection::Object* object
    );

public:
    static flatbuffers::Offset<FlatBuffers::IndexSchemaDescription> CreateSchemaDescription(
        flatbuffers::FlatBufferBuilder& builder,
        const Schema& schema
    );

    static flatbuffers::Offset<FlatBuffers::ProtocolBuffersMessageDescription> CreateProtocolBuffersObjectDescription(
        flatbuffers::FlatBufferBuilder& builder,
        const ProtocolBuffersObjectSchema& objectSchema
    );
    
    static flatbuffers::Offset<FlatBuffers::FlatBuffersObjectDescription> CreateFlatBuffersObjectDescription(
        flatbuffers::FlatBufferBuilder& builder,
        const FlatBuffersObjectSchema& objectSchema
    );

    static shared_ptr<const Schema> MakeSchema(
        FlatValue<FlatBuffers::IndexSchemaDescription> indexSchemaDescription
    );

    static shared_ptr<KeyComparer> MakeKeyComparer(
        std::shared_ptr<const Schema> schema);

    static shared_ptr<KeyComparer> MakeValueComparer(
        std::shared_ptr<const Schema> schema);

    static ProtoValue MakeProtoValueKey(
        const Schema& schema,
        const FlatBuffers::DataValue* dataValue,
        const FlatBuffers::ValuePlaceholder* placeholder = nullptr
    );

    static ProtoValue MakeProtoValueKey(
        const Schema& schema,
        AlignedMessageData alignedMessage,
        const FlatBuffers::ValuePlaceholder* placeholder = nullptr
    );

    static ProtoValue MakeProtoValueValue(
        const Schema& schema,
        AlignedMessageData alignedMessage
    );

    static ProtoValueComparers MakeComparers(
        std::shared_ptr<const Schema> schema
    );
};


}