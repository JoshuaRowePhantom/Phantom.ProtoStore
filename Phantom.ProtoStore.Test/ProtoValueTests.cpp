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
    EXPECT_EQ(nullptr, v.as_bytes_if().data());
}

TEST(ProtoValueTests, Construct_from_Message_pointer_with_pack_packs_message)
{
    google::protobuf::Empty e;
    ProtoValue v{ &e, true };

    EXPECT_EQ(get<const Message*>(v.message), &e);
    EXPECT_EQ(true, holds_alternative<std::string>(v.message_data));
    EXPECT_EQ(true, e.ParseFromString(get<std::string>(v.message_data)));
}

TEST(ProtoValueTests, Construct_from_span)
{
    std::byte byte;
    std::span<std::byte> bytes{ &byte, 1 };
    ProtoValue v(bytes);

    EXPECT_EQ(true, holds_alternative<std::monostate>(v.message));
    EXPECT_EQ(get<std::span<const std::byte>>(v.message_data).data(), bytes.data());
    EXPECT_EQ(get<std::span<const std::byte>>(v.message_data).size(), bytes.size());
    EXPECT_EQ(v.as_bytes_if().data(), bytes.data());
    EXPECT_EQ(v.as_bytes_if().size(), bytes.size());

}

TEST(ProtoValueTests, Construct_from_span_null_span_does_not_have_span)
{
    span<std::byte> bytes;
    ProtoValue v(bytes);

    EXPECT_EQ(true, holds_alternative<std::monostate>(v.message));
    EXPECT_EQ(true, holds_alternative<std::monostate>(v.message_data));
    EXPECT_EQ(v.as_bytes_if().data(), nullptr);
    EXPECT_EQ(v.as_bytes_if().size(), 0);
}

}