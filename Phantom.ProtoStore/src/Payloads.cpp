#include "Phantom.ProtoStore/Payloads.h"
#include "ProtoStoreInternal.pb.h"
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"

namespace Phantom::ProtoStore
{

extern const Serialization::PlaceholderKey KeyMinMessage;
extern const Serialization::PlaceholderKey KeyMaxMessage;
extern const std::span<const std::byte> KeyMinSpan;
extern const std::span<const std::byte> KeyMaxSpan;

ProtoValue ProtoValue::KeyMin()
{
    return &KeyMinMessage;
}

ProtoValue ProtoValue::KeyMax()
{
    return &KeyMaxMessage;
}

ProtoValue::ProtoValue() = default;

ProtoValue::ProtoValue(
    std::unique_ptr<google::protobuf::Message>&& other)
{
    if (other)
    {
        message = move(other);
    }
}

ProtoValue::ProtoValue(
    std::string bytes)
    :
    message_data(move(bytes))
{
}

ProtoValue::ProtoValue(
    std::span<const std::byte> bytes)
{
    if (bytes.data())
    {
        message_data = bytes;
    }
}

ProtoValue::ProtoValue(
    AlignedMessageData message)
{
    if (message)
    {
        message_data = std::move(message);
    }
}

ProtoValue::ProtoValue(
    const google::protobuf::Message* other,
    bool pack)
{
    if (other)
    {
        message = other;
        if (pack)
        {
            message_data = std::string{};
            other->SerializeToString(
                &get<std::string>(message_data));
        }
    }
}

ProtoValue::~ProtoValue() = default;

bool ProtoValue::IsKeyMin() const
{
    return as_message_if() == &KeyMinMessage
        || as_bytes_if().data() == KeyMinSpan.data();
}

bool ProtoValue::IsKeyMax() const
{
    return as_message_if() == &KeyMaxMessage
        || as_bytes_if().data() == KeyMaxSpan.data();
}

ProtoValue ProtoValue::pack_unowned() const
{
    ProtoValue result;
    result.message_data = this->message_data;
    result.message = this->as_message_if();

    if (IsKeyMin())
    {
        result.message_data = KeyMinSpan;
    }
    else if (IsKeyMax())
    {
        result.message_data = KeyMaxSpan;
    }
    else if (holds_alternative<std::monostate>(result.message_data)
        && result.as_message_if())
    {
        result.message_data = result.as_message_if()->SerializeAsString();
    }

    return std::move(result);
}

bool ProtoValue::pack(
    std::string* destination
) const
{
    auto bytes = as_bytes_if();
    if (bytes.data())
    {
        destination->assign(
            reinterpret_cast<const char*>(bytes.data()),
            bytes.size()
        );
        return true;
    }

    auto message = as_message_if();
    if (message)
    {
        return message->SerializeToString(
            destination);
    }

    return false;
}

void ProtoValue::unpack(
    google::protobuf::Message* destination
) const
{
    auto message = as_message_if();
    if (message)
    {
        destination->CopyFrom(*message);
        return;
    }

    auto bytes = as_bytes_if();
    if (bytes.data())
    {
        destination->ParseFromArray(
            bytes.data(),
            bytes.size_bytes()
        );
        return;
    }

    destination->Clear();
}

const google::protobuf::Message* ProtoValue::as_message_if() const
{
    {
        auto source = std::get_if<const google::protobuf::Message*>(&message);
        if (source)
        {
            return *source;
        }
    }

    {
        auto source = std::get_if<std::unique_ptr<const google::protobuf::Message>>(&message);
        if (source)
        {
            return source->get();
        }
    }

    return nullptr;
}

ProtoValue::operator bool() const
{
    return has_value();
}

bool ProtoValue::operator !() const
{
    return message.index() == 0
        && message_data.index() == 0;
}

bool ProtoValue::has_value() const
{
    return message.index() != 0
        || message_data.index() != 0;
}

const FlatBuffers::MessageHeader_V1* StoredMessage::Header_V1() const
{
    return
        ExtentFormatVersion == FlatBuffers::ExtentFormatVersion::V1
        ?
        get_struct<FlatBuffers::MessageHeader_V1>(Header.Payload)
        :
        nullptr;
}

std::optional<FlatBuffers::MessageReference_V1> StoredMessage::Reference_V1() const
{
    return Header_V1()
        ?
        std::optional{ FlatBuffers::MessageReference_V1(*Header_V1(), DataRange.Beginning) }
        :
        std::nullopt;
}

std::span<const std::byte> ProtoValue::as_bytes_if() const
{
    if (holds_alternative<std::string>(message_data))
    {
        return as_bytes(std::span{ get<std::string>(message_data) });
    }
    if (holds_alternative<std::span<const std::byte>>(message_data))
    {
        return get<std::span<const std::byte>>(message_data);
    }
    if (holds_alternative<AlignedMessageData>(message_data))
    {
        return get<AlignedMessageData>(message_data)->Payload;
    }
    return {};
}

ProtoValue::ProtoValue(
    ProtoValue&&
) = default;

ProtoValue& ProtoValue::operator=(ProtoValue&&) = default;

}