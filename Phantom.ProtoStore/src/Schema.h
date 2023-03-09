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

class Schema
{
    static void AddFileToMessageDescription(
        std::set<const google::protobuf::FileDescriptor*>& addedFileDescriptors,
        google::protobuf::FileDescriptorSet* fileDescriptorSet,
        const google::protobuf::FileDescriptor* fileDescriptor);

public:
    static void MakeSchemaDescription(
        Serialization::SchemaDescription& messageDescription,
        const Descriptor* messageDescriptor
    );

    static shared_ptr<IMessageFactory> MakeMessageFactory(
        const Message* prototype);

    static shared_ptr<IMessageFactory> MakeMessageFactory(
        const Serialization::ProtocolBuffersMessageDescription& messageDescription);
};


}