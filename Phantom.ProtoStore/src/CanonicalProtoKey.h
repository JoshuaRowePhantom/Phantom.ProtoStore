#include "StandardTypes.h"
#include <compare>
#include <concepts>
#include "google/protobuf/descriptor.h"
#include "google/protobuf/io/zero_copy_stream.h"
#include "Schema.h"

namespace Phantom::ProtoStore
{

class CanonicalProtoKey
{
    shared_ptr<IMessageFactory> m_messageFactory;

public:
    CanonicalProtoKey(
        shared_ptr<IMessageFactory> messageFactory
    );

    void Rewrite(
        google::protobuf::io::ZeroCopyInputStream* input,
        google::protobuf::io::ZeroCopyOutputStream* output
    );

    bool IsCanonical(
        google::protobuf::io::ZeroCopyInputStream* input
    );
};

}
