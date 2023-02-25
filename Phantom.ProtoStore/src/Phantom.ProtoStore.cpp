// Phantom.ProtoStore.cpp : Defines the entry point for the application.
//

#include "Phantom.ProtoStore/Phantom.ProtoStore.h"
#include "ProtoStoreInternal.pb.h"

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
    std::unique_ptr<Message>&& other)
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
    RawData bytes)
{
    if (bytes->data())
    {
        message_data = std::move(bytes);
    }
}

ProtoValue::ProtoValue(
    const Message* other,
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
    Message* destination
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

const Message* ProtoValue::as_message_if() const
{
    {
        auto source = std::get_if<const Message*>(&message);
        if (source)
        {
            return *source;
        }
    }

    {
        auto source = std::get_if<unique_ptr<const Message>>(&message);
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
    if (holds_alternative<RawData>(message_data))
    {
        return get<RawData>(message_data).data();
    }
    return {};
}

ProtoValue::ProtoValue(
    ProtoValue&&
) = default;

ProtoValue& ProtoValue::operator=(ProtoValue&&) = default;

OpenProtoStoreRequest::OpenProtoStoreRequest()
{
    DefaultMergeParameters.set_mergesperlevel(
        10);
    DefaultMergeParameters.set_maxlevel(
        10);
}

class ProtoStoreErrorCategoryImpl : public std::error_category
{
    virtual const char* name() const noexcept override
    {
        return "ProtoStore";
    }

    virtual std::string message(int errorValue) const override
    {
        static std::string AbortedTransaction = "Aborted transaction";
        static std::string WriteConflict = "Write conflict";
        static std::string UnresolvedTransaction = "Unresolved transaction";

        switch (static_cast<ProtoStoreErrorCode>(errorValue))
        {
        case ProtoStoreErrorCode::AbortedTransaction:
            return AbortedTransaction;
        case ProtoStoreErrorCode::WriteConflict:
            return WriteConflict;
        case ProtoStoreErrorCode::UnresolvedTransaction:
            return UnresolvedTransaction;
        default:
            return "Unknown error";
        }
    }
};

const std::error_category& ProtoStoreErrorCategory()
{
    static ProtoStoreErrorCategoryImpl result;
    return result;
}

std::error_code make_error_code(
    ProtoStoreErrorCode errorCode
)
{
    return std::error_code{ static_cast<int>(errorCode), ProtoStoreErrorCategory() };
}

std::unexpected<std::error_code> make_unexpected(
    ProtoStoreErrorCode errorCode
)
{
    return std::unexpected{ make_error_code(errorCode) };
}

std::unexpected<std::error_code> abort_transaction()
{
    return make_unexpected(ProtoStoreErrorCode::AbortedTransaction);
}

}
