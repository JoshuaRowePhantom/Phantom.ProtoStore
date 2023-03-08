#pragma once

#include <flatbuffers/flatbuffers.h>
#include <google/protobuf/message.h>
#include <memory>
#include <span>
#include <stdint.h>
#include <variant>
#include "Primitives.h"

namespace Phantom::ProtoStore
{
namespace FlatBuffers
{
enum class ExtentFormatVersion : int8_t;
struct MessageHeader_V1;
struct MessageReference_V1;
struct PartitionMessage;
}

// A DataReference uses an std::shared_ptr to ensure
// access to an backing store is maintained at least
// as long as the resource is referenced.
// An example use is to hold open a memory mapped file
// as the backing store for a FlatBuffers table read from that file.
template<
    typename Data
>
class DataReference
{
    template<
        typename Data
    >
    friend class DataReference;

    std::shared_ptr<void> m_dataHolder;
    Data m_data;

public:
    DataReference(
        nullptr_t = nullptr
    )
    {}

    DataReference(
        std::shared_ptr<void> dataHolder,
        Data data
    ) noexcept :
        m_dataHolder{ std::move(dataHolder) },
        m_data{ std::move(data) }
    {}

    DataReference(
        const DataReference&
    ) = default;

    DataReference(
        DataReference&& other
    ) :
        m_dataHolder{ std::move(other.m_dataHolder) },
        m_data{ std::move(other.m_data) }
    {
        other.m_data = {};
    }

    template<
        typename Other
    >
    explicit DataReference(
        DataReference<Other>&& other,
        Data data
    ) :
        m_dataHolder{ std::move(other.m_dataHolder) },
        m_data{ std::move(data) }
    {
        other.m_data = Other{};
    }

    template<
        typename Other
    >
    explicit DataReference(
        const DataReference<Other>& other,
        Data data
    ) :
        m_dataHolder{ other.m_dataHolder },
        m_data{ std::move(data) }
    {
    }

    DataReference& operator=(const DataReference& other) = default;

    auto& operator=(DataReference&& other)
    {
        if (&other != this)
        {
            m_dataHolder = std::move(other.m_dataHolder);
            m_data = other.m_data;
            other.m_dataHolder = nullptr;
            other.m_data = {};
        }

        return *this;
    }

    operator bool() const noexcept
        requires std::convertible_to<Data, bool>
    {
        return static_cast<bool>(m_data);
    }

    const Data& data() const noexcept
    {
        return m_data;
    }

    const Data* operator->() const noexcept
    {
        return std::addressof(m_data);
    }

    const Data& operator*() const noexcept
    {
        return m_data;
    }
};


struct AlignedMessage
{
    uint8_t Alignment = 0;
    std::span<const std::byte> Payload;

    explicit operator bool() const
    {
        return Payload.data();
    }

    AlignedMessage()
    {
    }

    AlignedMessage(
        uint8_t alignment,
        std::span<const std::byte> payload
    ) :
        Alignment{ alignment },
        Payload{ payload }
    {}

    AlignedMessage(
        const flatbuffers::FlatBufferBuilder& builder
    ) :
        Alignment(builder.GetBufferMinAlignment()),
        Payload(as_bytes(builder.GetBufferSpan()))
    {}
};

using RawData = DataReference<std::span<const std::byte>>;
using WritableRawData = DataReference<std::span<std::byte>>;

std::span<const std::byte> get_byte_span(
    const flatbuffers::Vector<int8_t>*
);

std::span<const std::byte> get_byte_span(
    const std::string&
);

std::span<const char> get_char_span(
    std::span<const std::byte>
);

std::span<const uint8_t> get_uint8_t_span(
    std::span<const std::byte>
);

std::span<const int8_t> get_int8_t_span(
    std::span<const std::byte>
);

template<typename T>
const T* get_struct(
    std::span<const std::byte> span)
{
    if (!span.data())
    {
        return nullptr;
    }
    assert(span.size() == sizeof(T));
    return reinterpret_cast<const T*>(span.data());
}

typedef std::uint64_t ExtentOffset;

struct ExtentOffsetRange
{
    ExtentOffset Beginning;
    ExtentOffset End;
};

struct StoredMessage
{
    // The format version of the extent the message was stored in.
    FlatBuffers::ExtentFormatVersion ExtentFormatVersion;
    
    AlignedMessage Header;
    AlignedMessage Content;

    // The range the message was stored in.
    ExtentOffsetRange DataRange;

    operator bool() const
    {
        return Content.operator bool();
    }

    const FlatBuffers::MessageHeader_V1* Header_V1() const;
    std::optional<FlatBuffers::MessageReference_V1> Reference_V1() const;
};

template<
    typename T
> concept IsNativeTable = std::derived_from<T, flatbuffers::NativeTable>;

template<
    typename Table
> class FlatMessage;

template<
    typename T
> concept IsFlatMessage = is_template_instantiation<T, FlatMessage>;

template<
    typename Table
> class FlatMessage
{
    DataReference<StoredMessage> m_storedMessage;
    const Table* m_table;

public:
    FlatMessage()
    {}

    explicit FlatMessage(
        DataReference<StoredMessage> storedMessage
    ) :
        m_storedMessage{ std::move(storedMessage) },
        m_table
        {
            m_storedMessage
            ?
            flatbuffers::GetRoot<Table>(m_storedMessage->Content.Payload.data())
            :
            nullptr
        }
    {
        DebugVerifyBuffer();
    }

    explicit FlatMessage(
        DataReference<StoredMessage> storedMessage,
        const Table* table
    ) :
        m_storedMessage{ std::move(storedMessage) },
        m_table{ table }
    {
        DebugVerifyBuffer();
    }

    template<
        typename Other
    >
    explicit FlatMessage(
        const FlatMessage<Other>& other,
        const Table* table
    ) :
        m_storedMessage{ other },
        m_table{ table }
    {
        DebugVerifyTable();
    }

    explicit FlatMessage(
        uint8_t messageAlignment,
        std::span<const std::byte> message
    ) :
        m_storedMessage
    {
        nullptr,
        {
            .ExtentFormatVersion = FlatBuffers::ExtentFormatVersion::None,
            .Content = { messageAlignment, message },
        },
    },
    m_table
    {
        flatbuffers::GetRoot<Table>(
            m_storedMessage->Content.Payload)
    }
    {
        DebugVerifyBuffer();
    }

    explicit FlatMessage(
        const flatbuffers::FlatBufferBuilder& builder
    ) :
        m_storedMessage
    {
        nullptr,
        {
            .ExtentFormatVersion = FlatBuffers::ExtentFormatVersion::None,
            .Content { builder },
        },
    },
    m_table
    {
        flatbuffers::GetRoot<Table>(
            m_storedMessage->Content.Payload.data())
    }
    {
        DebugVerifyBuffer();
    }

    explicit FlatMessage(
        std::shared_ptr<flatbuffers::FlatBufferBuilder> builder
    ) :
        m_storedMessage
    {
        builder,
        {
            .ExtentFormatVersion = FlatBuffers::ExtentFormatVersion::None,
            .Content { *builder },
        },
    },
    m_table
    {
        flatbuffers::GetRoot<Table>(
            m_storedMessage->Content.Payload.data())
    }
    {
        DebugVerifyBuffer();
    }

    explicit FlatMessage(
        const typename Table::NativeTableType* table
    )
    {
        flatbuffers::FlatBufferBuilder builder;
        auto rootOffset = Table::Pack(
            builder,
            table);
        builder.Finish(rootOffset);

        StoredMessage storedMessage =
        {
            .ExtentFormatVersion = static_cast<Phantom::ProtoStore::FlatBuffers::ExtentFormatVersion>(0),
            .Content { builder }
        };

        size_t size;
        size_t offset;

        m_storedMessage =
        {
            std::shared_ptr<uint8_t[]>{ builder.ReleaseRaw(size, offset) },
            storedMessage,
        };

        m_table = flatbuffers::GetRoot<Table>(
            m_storedMessage->Content.Payload.data());

        DebugVerifyBuffer();
    }

    const StoredMessage& data() const noexcept
    {
        return *m_storedMessage;
    }

    const Table* get() const noexcept
    {
        return m_table;
    }

    const Table& operator*() const noexcept
    {
        return *m_table;
    }

    explicit operator bool() const noexcept
    {
        return m_table;
    }

    const Table* operator->() const noexcept
    {
        return get();
    }

    explicit operator const DataReference<StoredMessage>& () const noexcept
    {
        return m_storedMessage;
    }

    void DebugVerifyBuffer()
    {
        if (m_storedMessage)
        {
            flatbuffers::Verifier verifier(
                get_uint8_t_span(m_storedMessage->Content.Payload).data(),
                m_storedMessage->Content.Payload.size());
            assert(verifier.VerifyBuffer<Table>());
        }
    }

    void DebugVerifyTable()
    {
        if (m_table)
        {
            flatbuffers::Verifier verifier(
                get_uint8_t_span(m_storedMessage->Content.Payload).data(),
                m_storedMessage->Content.Payload.size());
            assert(verifier.VerifyTable(m_table));
        }
    }
};

template<
    IsNativeTable NativeTable
> FlatMessage(const NativeTable*) -> FlatMessage<typename NativeTable::TableType>;

template<
    typename Table,
    typename Other
> FlatMessage(const FlatMessage<Other>&, const Table*) -> FlatMessage<Table>;

class ProtoValue
{
    typedef std::variant<
        std::monostate,
        std::span<const std::byte>,
        std::string,
        RawData
    > message_data_type;

    typedef std::variant<
        std::monostate,
        const google::protobuf::Message*,
        std::unique_ptr<const google::protobuf::Message>
    > message_type;

public:

    message_data_type message_data;
    message_type message;

    ProtoValue();

    ProtoValue(
        std::unique_ptr<google::protobuf::Message>&& other);

    ProtoValue(
        std::string bytes);

    ProtoValue(
        std::span<const std::byte> bytes);

    ProtoValue(
        RawData bytes);

    ProtoValue(
        const google::protobuf::Message* other,
        bool pack = false);

    ProtoValue(
        ProtoValue&&);

    ~ProtoValue();

    static ProtoValue KeyMin();
    static ProtoValue KeyMax();

    explicit operator bool() const;
    bool operator !() const;
    bool has_value() const;

    const google::protobuf::Message* as_message_if() const;

    template<
        typename TMessage
    > const TMessage* cast_if() const
    {
        auto message = as_message_if();
        if constexpr (std::same_as<google::protobuf::Message, TMessage>)
        {
            return message;
        }
        else if (message
            &&
            message->GetDescriptor() == TMessage::descriptor())
        {
            return static_cast<const TMessage*>(message);
        }

        return nullptr;
    }

    void unpack(
        google::protobuf::Message* destination
    ) const;

    bool pack(
        std::string* destination
    ) const;

    ProtoValue pack_unowned() const;

    std::span<const std::byte> as_bytes_if() const;

    bool IsKeyMin() const;
    bool IsKeyMax() const;

    ProtoValue& operator=(ProtoValue&&);
};

}