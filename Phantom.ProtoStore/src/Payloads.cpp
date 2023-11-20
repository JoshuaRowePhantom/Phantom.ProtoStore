#include "Phantom.ProtoStore/Payloads.h"
#include "ProtoStoreInternal.pb.h"
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"
#include "Phantom.ProtoStore/numeric_cast.h"
#include "Checksum.h"

namespace Phantom::ProtoStore
{

ProtoValue ProtoValue::KeyMin()
{
    ProtoValue result;
    result.message.emplace<key_min>();
    return result;
}

ProtoValue ProtoValue::KeyMax()
{
    ProtoValue result;
    result.message.emplace<key_max>();
    return result;
}

ProtoValue::ProtoValue() = default;

ProtoValue::~ProtoValue() = default;

ProtoValue::ProtoValue(
    ProtoValue&& other
)
{
    std::swap(message_data, other.message_data);
    std::swap(message, other.message);
}

ProtoValue::ProtoValue(
    const ProtoValue& other
) = default;

ProtoValue& ProtoValue::operator=(const ProtoValue&) = default;

ProtoValue& ProtoValue::operator=(ProtoValue&& other)
{
    if (&other == this)
    {
        return *this;
    }

    ProtoValue temp(std::move(*this));
    std::swap(message_data, other.message_data);
    std::swap(message, other.message);

    return *this;
}

ProtoValue::ProtoValue(
    flat_buffer_message flatBufferMessage
)
    :
    ProtoValue
{
    backing_store{},
    std::move(flatBufferMessage)
}
{
}

ProtoValue::ProtoValue(
    const google::protobuf::Message* protocolBufferMessage
) :
    ProtoValue
{
    protocol_buffer_message{ protocolBufferMessage }
}
{}

ProtoValue::ProtoValue(
    protocol_buffer_message protocolBufferMessage
) :
    ProtoValue
{
    backing_store{},
    std::move(protocolBufferMessage)
}
{}

ProtoValue::ProtoValue(
    backing_store backingStore,
    protocol_buffer_message protocolBufferMessage
)
{
    switch (backingStore.index())
    {
    case 0:
        break;
    case 1:
        if (get<1>(backingStore).data())
        {
            message_data.emplace<protocol_buffers_span>(get<1>(std::move(backingStore)));
        }
        break;
    case 2:
        message_data.emplace<protocol_buffers_string>(get<2>(std::move(backingStore)));
        break;
    case 3:
        if (get<3>(backingStore))
        {
            message_data.emplace<protocol_buffers_aligned_message_data>(get<3>(std::move(backingStore)));
        }
        break;
    }

    switch (protocolBufferMessage.index())
    {
    case 1:
        if (get<1>(protocolBufferMessage))
        {
            message.emplace<protocol_buffers_message_pointer>(get<1>(std::move(protocolBufferMessage)));
        }
        break;
    case 2:
        if (get<2>(protocolBufferMessage))
        {
            message.emplace<protocol_buffers_message_shared_ptr>(get<2>(std::move(protocolBufferMessage)));
        }
        break;
    }
}

ProtoValue ProtoValue::ProtocolBuffer(
    backing_store backingStore,
    protocol_buffer_message protocolBufferMessage
)
{
    return 
    {
        std::move(backingStore),
        std::move(protocolBufferMessage),
    };
}

ProtoValue ProtoValue::ProtocolBuffer(
    protocol_buffer_message protocolBufferMessage
)
{
    return
    {
        {},
        std::move(protocolBufferMessage),
    };
}

ProtoValue::ProtoValue(
    backing_store backingStore,
    flat_buffer_message flatBufferMessage)
{
    switch (backingStore.index())
    {
    case 0:
        break;
    case 1:
        if (get<1>(backingStore).data())
        {
            message_data.emplace<flat_buffers_span>(get<1>(std::move(backingStore)));
        }
        break;
    case 2:
        message_data.emplace<flat_buffers_string>(get<2>(std::move(backingStore)));
        break;
    case 3:
        if (get<3>(backingStore))
        {
            message_data.emplace<flat_buffers_aligned_message_data>(get<3>(std::move(backingStore)));
        }
        break;
    }

    switch (flatBufferMessage.index())
    {
    case 1:
        if (get<1>(flatBufferMessage))
        {
            message.emplace<flat_buffers_table_pointer>(get<1>(std::move(flatBufferMessage)));
        }
        break;
    }

    if (!as_table_if()
        && as_flat_buffer_bytes_if().data())
    {
        message.emplace<flat_buffers_table_pointer>(
            flatbuffers::GetRoot<flatbuffers::Table>(
                as_flat_buffer_bytes_if().data()));
    }
}

ProtoValue ProtoValue::FlatBuffer(
    backing_store backingStore,
    flat_buffer_message flatBufferMessage
)
{
    return
    {
        std::move(backingStore),
        std::move(flatBufferMessage),
    };
}

ProtoValue ProtoValue::FlatBuffer(
    flat_buffer_message flatBufferMessage
)
{
    return
    {
        {},
        std::move(flatBufferMessage),
    };
}

ProtoValue ProtoValue::FlatBuffer(
    flatbuffers::FlatBufferBuilder builder
)
{
    auto alignment = static_cast<uint8_t>(builder.GetBufferMinAlignment());
    auto detachedBuffer = std::make_shared<flatbuffers::DetachedBuffer>(
        builder.Release());
    auto span = std::span<const uint8_t>
    {
        detachedBuffer->data(),
        detachedBuffer->size()
    };

    AlignedMessage alignedMessage
    {
        alignment,
        as_bytes(span),
    };

    return FlatBuffer(
        AlignedMessageData
        {
            detachedBuffer,
            alignedMessage
        }
    );
}

ProtoValue::ProtoValue(
    backing_store backingStore,
    const google::protobuf::Message* protocolMessage
) : ProtoValue
{
    std::move(backingStore),
    protocol_buffer_message { protocolMessage },
}
{
}

bool ProtoValue::IsKeyMin() const
{
    return message.index() == key_min;
}

bool ProtoValue::IsKeyMax() const
{
    return message.index() == key_max;
}

ProtoValue&& ProtoValue::pack()&&
{
    return std::move(this->pack());
}

ProtoValue& ProtoValue::pack()&
{
    if (IsKeyMax() || IsKeyMin())
    {
        return *this;
    }

    if (message_data.index() == 0
        && as_message_if())
    {
        message_data.emplace<protocol_buffers_string>(
            as_message_if()->SerializeAsString());
    }

    return *this;
}

ProtoValue ProtoValue::pack_unowned() const
{
    return ProtoValue(*this).pack();
}

bool ProtoValue::pack(
    std::string* destination
) const
{
    auto bytes = as_protocol_buffer_bytes_if();
    if (bytes.data())
    {
        destination->assign(
            reinterpret_cast<const char*>(bytes.data()),
            bytes.size()
        );
        return true;
    }

    auto asMessageIf = as_message_if();
    if (asMessageIf)
    {
        return asMessageIf->SerializeToString(
            destination);
    }

    return false;
}

void ProtoValue::unpack(
    google::protobuf::Message* destination
) const
{
    auto asMessageIf = as_message_if();
    if (asMessageIf)
    {
        destination->CopyFrom(*asMessageIf);
        return;
    }

    auto bytes = as_protocol_buffer_bytes_if();
    if (bytes.data())
    {
        destination->ParseFromArray(
            bytes.data(),
            numeric_cast(bytes.size_bytes())
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
        auto source = std::get_if<std::shared_ptr<const google::protobuf::Message>>(&message);
        if (source)
        {
            return source->get();
        }
    }

    return nullptr;
}

const flatbuffers::Table* ProtoValue::as_table_if() const
{
    if (holds_alternative<const flatbuffers::Table*>(message))
    {
        return get<const flatbuffers::Table*>(message);
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

void StoredMessage::VerifyChecksum() const
{
    if (!this->Content && !this->Header_V1())
    {
        return;
    }

    auto crc = checksum_v1(this->Content.Payload);
    if (crc != this->Header_V1()->crc32())
    {
        throw std::range_error("crc");
    }
}

std::span<const std::byte> ProtoValue::as_protocol_buffer_bytes_if() const
{
    switch (message_data.index())
    {
    case protocol_buffers_aligned_message_data:
        return get<protocol_buffers_aligned_message_data>(message_data)->Payload;
    case protocol_buffers_span:
        return get<protocol_buffers_span>(message_data);
    case protocol_buffers_string:
        return as_bytes(std::span{ get<protocol_buffers_string>(message_data) });
    }
    return {};
}

std::span<const std::byte> ProtoValue::as_flat_buffer_bytes_if() const
{
    switch (message_data.index())
    {
    case flat_buffers_aligned_message_data:
        return get<flat_buffers_aligned_message_data>(message_data)->Payload;
    case flat_buffers_span:
        return get<flat_buffers_span>(message_data);
    case flat_buffers_string:
        return as_bytes(std::span{ get<flat_buffers_string>(message_data) });
    }
    return {};
}

AlignedMessage ProtoValue::as_aligned_message_if() const
{
    switch (message_data.index())
    {
    case protocol_buffers_aligned_message_data:
        return *get<protocol_buffers_aligned_message_data>(message_data);
    case flat_buffers_aligned_message_data:
        return *get<flat_buffers_aligned_message_data>(message_data);
    }

    auto protocolBufferBytes = as_protocol_buffer_bytes_if();
    if (protocolBufferBytes.data())
    {
        return AlignedMessage
        {
            1,
            protocolBufferBytes
        };
    }

    auto flatBufferBytes = as_flat_buffer_bytes_if();
    if (protocolBufferBytes.data())
    {
        return AlignedMessage
        {
            32,
            protocolBufferBytes
        };
    }

    return AlignedMessage{};
}


bool ProtoValue::is_protocol_buffer() const
{
    return
        message_data.index() == protocol_buffers_aligned_message_data
        || message_data.index() == protocol_buffers_span
        || message_data.index() == protocol_buffers_string
        || message.index() == protocol_buffers_message_pointer
        || message.index() == protocol_buffers_message_shared_ptr;
}

bool ProtoValue::is_flat_buffer() const
{
    return
        message.index() == flat_buffers_table_pointer;
}

template<
    typename Function
> std::function<Function> MakeUnowningFunctor(
    const std::function<Function>* function
)
{
    return [=](auto&&... args)
    {
        return (*function)(std::forward<decltype(args)>(args)...);
    };
}

ProtoValueComparers ProtoValueComparers::MakeUnowningCopy() const
{
    return ProtoValueComparers
    {
        MakeUnowningFunctor(&comparer),
        MakeUnowningFunctor(&equal_to),
        MakeUnowningFunctor(&hash),
        MakeUnowningFunctor(&less),
        MakeUnowningFunctor(&prefix_comparer),
    };
}
}