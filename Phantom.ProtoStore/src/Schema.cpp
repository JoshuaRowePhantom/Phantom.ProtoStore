#include "Schema.h"
#include "ProtoStoreInternal.pb.h"
#include "KeyComparer.h"
#include <google/protobuf/dynamic_message.h>
#include <array>

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

shared_ptr<KeyComparer> SchemaDescriptions::MakeKeyComparer(
    const Serialization::SchemaDescription& messageDescription)
{
    if (messageDescription.has_protocolbuffersdescription())
    {
        return MakeProtocolBuffersKeyComparer(
            messageDescription.protocolbuffersdescription());
    }

    throw std::range_error("messageDescription");
}

shared_ptr<KeyComparer> SchemaDescriptions::MakeProtocolBuffersKeyComparer(
    const Serialization::ProtocolBuffersSchemaDescription& protocolBuffersSchemaDescription)
{
    auto& messageDescription = protocolBuffersSchemaDescription.messagedescription();
    auto generatedDescriptorPool = google::protobuf::DescriptorPool::generated_pool();
    auto generatedDescriptor = generatedDescriptorPool->FindMessageTypeByName(
        messageDescription.messagename());

    if (generatedDescriptor)
    {
        return std::make_shared<ProtoKeyComparer>(
            generatedDescriptor);
    }

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

    struct holder
    {
        std::shared_ptr< google::protobuf::DescriptorPool> descriptorPool;
        std::shared_ptr<KeyComparer> keyComparer;
    };

    auto holderPointer = std::make_shared<holder>(
        holder {
            descriptorPool,
            std::make_shared<ProtoKeyComparer>(
                messageDescriptor)
        });

    return std::shared_ptr<KeyComparer>(
        holderPointer,
        holderPointer->keyComparer.get()
        );
}

}