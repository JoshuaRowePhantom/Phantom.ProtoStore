#include "StandardIncludes.h"

#include <google/protobuf/empty.pb.h>

namespace Phantom::ProtoStore
{
TEST(ProtoValueTests, Construct_from_Message_pointer)
{
    google::protobuf::Empty e;
    ProtoValue v = &e;

    EXPECT_EQ(get<const Message*>(v.message), &e);
    EXPECT_EQ(true, holds_alternative<std::monostate>(v.message_data));
}

TEST(ProtoValueTests, Construct_from_span)
{
    span<byte> bytes;
    ProtoValue v(bytes);

    EXPECT_EQ(true, holds_alternative<std::monostate>(v.message));
    EXPECT_EQ(get<span<const byte>>(v.message_data).data(), bytes.data());
    EXPECT_EQ(get<span<const byte>>(v.message_data).size(), bytes.size());
}
}