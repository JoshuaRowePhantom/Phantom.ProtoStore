#include "Schema.h"
#include "src/ProtoStoreInternal.pb.h"
#include <google/protobuf/dynamic_message.h>
#include <array>

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

shared_ptr<IMessageFactory> Schema::MakeMessageFactory(
    const Message* prototype)
{
    return make_shared<ProtoStoreMessageFactory>(
        prototype,
        prototype->GetDescriptor(),
        std::any());
}

shared_ptr<IMessageFactory> Schema::MakeMessageFactory(
    const MessageDescription& messageDescription
)
{
    auto generatedDescriptorPool = google::protobuf::DescriptorPool::generated_pool();
    auto generatedDescriptor = generatedDescriptorPool->FindMessageTypeByName(
        messageDescription.messagename());

    if (generatedDescriptor)
    {
        auto generatedMessageFactory = google::protobuf::MessageFactory::generated_factory();
        auto generatedPrototype = generatedMessageFactory->GetPrototype(
            generatedDescriptor);

        return MakeMessageFactory(
            generatedPrototype);
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

    auto messageFactory = make_shared<google::protobuf::DynamicMessageFactory>();
    auto protoType = messageFactory->GetPrototype(
        messageDescriptor);

    // Construct an object to hold the shared pointers
    // that will be destroyed in the correct order.
    std::array holders =
    {
        std::any(descriptorPool),
        std::any(messageFactory),
    };

    return make_shared<ProtoStoreMessageFactory>(
        protoType,
        messageDescriptor,
        holders);
}

}