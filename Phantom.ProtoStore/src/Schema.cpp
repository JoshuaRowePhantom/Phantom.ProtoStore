#include "Schema.h"
#include "src/ProtoStoreInternal.pb.h"

namespace Phantom::ProtoStore
{

void Schema::AddFileToMessageDescription(
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

void Schema::MakeMessageDescription(
    MessageDescription& messageDescription,
    const Descriptor* messageDescriptor
)
{
    messageDescription.Clear();
    messageDescription.set_messagename(
        messageDescriptor->full_name());
    
    std::set<const google::protobuf::FileDescriptor*> addedFileDescriptors;

    AddFileToMessageDescription(
        addedFileDescriptors,
        messageDescription.mutable_filedescriptors(),
        messageDescriptor->file()
    );
}

}