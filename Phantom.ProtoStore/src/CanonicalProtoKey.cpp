#include "CanonicalProtoKey.h"

namespace Phantom::ProtoStore
{

CanonicalProtoKey::CanonicalProtoKey(
    shared_ptr<IMessageFactory> messageFactory
) :
    m_messageFactory{ std::move(messageFactory) }
{}

void CanonicalProtoKey::Rewrite(
    google::protobuf::io::ZeroCopyInputStream* input,
    google::protobuf::io::ZeroCopyOutputStream* output
)
{
    throw 0;
}

bool CanonicalProtoKey::IsCanonical(
    google::protobuf::io::ZeroCopyInputStream* input
)
{
    return true;
}

}