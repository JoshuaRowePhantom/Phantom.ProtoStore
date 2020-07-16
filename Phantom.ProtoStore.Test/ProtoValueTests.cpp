#include "StandardIncludes.h"

#include <google/protobuf/empty.pb.h>

namespace Phantom::ProtoStore
{
TEST(ProtoValueTests, Construct_from_Message_pointer)
{
    google::protobuf::Empty e;
    ProtoValue v = &e;

    ASSERT_EQ(get<const Message*>(v.message), &e);
    ASSERT_EQ(true, holds_alternative<std::monostate>(v.message_data));
}

TEST(ProtoValueTests, Construct_from_span)
{
    span<byte> bytes;
    ProtoValue v = bytes;

    ASSERT_EQ(true, holds_alternative<std::monostate>(v.message));
    ASSERT_EQ(get<span<const byte>>(v.message_data).data(), bytes.data());
    ASSERT_EQ(get<span<const byte>>(v.message_data).size(), bytes.size());
}
}