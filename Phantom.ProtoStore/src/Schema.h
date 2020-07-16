#pragma once

#include "StandardTypes.h"
#include <set>

namespace Phantom::ProtoStore
{

class Schema
{
    static void AddFileToMessageDescription(
        std::set<const google::protobuf::FileDescriptor*>& addedFileDescriptors,
        google::protobuf::FileDescriptorSet* fileDescriptorSet,
        const google::protobuf::FileDescriptor* fileDescriptor);

public:
    static void MakeMessageDescription(
        MessageDescription& messageDescription,
        const Descriptor* messageDescriptor
    );
};
}