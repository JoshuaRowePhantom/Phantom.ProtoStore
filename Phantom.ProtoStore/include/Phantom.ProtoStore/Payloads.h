#pragma once

#include <flatbuffers/flatbuffers.h>
#include <google/protobuf/message.h>
#include <memory>
#include <span>
#include <stdint.h>
#include <variant>
#include "Primitives.h"
#include "Phantom.System/concepts.h"

namespace Phantom::ProtoStore
{
namespace FlatBuffers
{
enum class ExtentFormatVersion : int8_t;
struct MessageHeader_V1;
struct MessageReference_V1;
struct PartitionMessage;
}

class ProtoValue;

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

    explicit operator bool() const noexcept
        requires requires (const Data d) { static_cast<bool>(d); }
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
using AlignedMessageData = DataReference<AlignedMessage>;

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
> concept IsFlatBufferTable = std::is_base_of<flatbuffers::Table, T>::value;

template<
    typename T
> concept IsProtocolBufferMessage = std::is_base_of<google::protobuf::Message, T>::value;

template<
    typename T
> concept IsNativeTable = std::derived_from<T, flatbuffers::NativeTable>;

template<
    IsFlatBufferTable Table
> class FlatMessage;

template<
    typename T
> concept IsFlatMessage = is_template_instantiation<T, FlatMessage>;

template<
    IsFlatBufferTable Table
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

    template<
        IsNativeTable NativeTable
    >
        requires std::same_as<NativeTable, typename Table::NativeTableType>
    explicit FlatMessage(
        const NativeTable* table
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

    operator ProtoValue() const & noexcept;
    operator ProtoValue()&& noexcept;

    void DebugVerifyBuffer()
    {
        if constexpr (!std::same_as<flatbuffers::Table, Table>)
        {
            if (m_storedMessage)
            {
                flatbuffers::Verifier verifier(
                    get_uint8_t_span(m_storedMessage->Content.Payload).data(),
                    m_storedMessage->Content.Payload.size());
                assert(verifier.VerifyBuffer<Table>());
            }
        }
    }

    void DebugVerifyTable()
    {
        if constexpr (!std::same_as<flatbuffers::Table, Table>)
        {
            if (m_table)
            {
                flatbuffers::Verifier verifier(
                    get_uint8_t_span(m_storedMessage->Content.Payload).data(),
                    m_storedMessage->Content.Payload.size());
                assert(verifier.VerifyTable(m_table));
            }
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
    static constexpr size_t nothing = 0;

    // These constants correspond to the values in message_data_type variant.
    static constexpr size_t protocol_buffers_span = 1;
    static constexpr size_t protocol_buffers_string = 2;
    static constexpr size_t protocol_buffers_aligned_message_data = 3;
    static constexpr size_t flat_buffers_span = 4;
    static constexpr size_t flat_buffers_string = 5;
    static constexpr size_t flat_buffers_aligned_message_data = 6;

    // The backing store for a message.
    typedef std::variant<
        std::monostate,

        // protocol buffer values
        std::span<const std::byte>,
        std::string,
        AlignedMessageData,
        
        // flat buffer values
        std::span<const std::byte>,
        std::string,
        AlignedMessageData
    > message_data_type;

    // These constants correspond to the values in message_type variant.
    static constexpr size_t protocol_buffers_message_pointer = 1;
    static constexpr size_t protocol_buffers_message_shared_ptr = 2;
    static constexpr size_t flat_buffers_table_pointer = 3;
    static constexpr size_t key_min = 4;
    static constexpr size_t key_max = 5;

    // The actual message represented in a ProtoValue.
    typedef std::variant<
        std::monostate,
        // protocol buffer values
        const google::protobuf::Message*,
        std::shared_ptr<const google::protobuf::Message>,
        // flat buffer values
        const flatbuffers::Table*,
        // placeholder key values
        // key-min
        std::monostate,
        // key-max
        std::monostate
    > message_type;

    message_data_type message_data;
    message_type message;

    static constexpr size_t backing_store_span = 1;
    static constexpr size_t backing_store_string = 2;
    static constexpr size_t backing_store_aligned_message_data = 3;

public:

    using backing_store = std::variant<
        std::monostate,
        std::span<const std::byte>,
        std::string,
        AlignedMessageData
    >;

public:

    using backing_store = std::variant<
        std::monostate,
        std::span<const std::byte>,
        std::string,
        AlignedMessageData
    >;

    using protocol_buffer_message = std::variant<
        std::monostate,
        const google::protobuf::Message*,
        std::shared_ptr<const google::protobuf::Message>
    >;

    using flat_buffer_message = std::variant<
        std::monostate,
        const flatbuffers::Table*
    >;

    ProtoValue();

    ProtoValue(
        const ProtoValue&);

    ProtoValue(
        ProtoValue&&);

    ProtoValue& operator=(ProtoValue&&);

    ProtoValue(
        flat_buffer_message flatBufferMessage
    );

    template<
        IsFlatBufferTable Table
    >
    ProtoValue(
        const Table* flatBufferMessage
    )
        :
        ProtoValue(
            flat_buffer_message{ reinterpret_cast<const flatbuffers::Table*>(flatBufferMessage) })
    {
    }

    ProtoValue(
        backing_store backingStore,
        flat_buffer_message flatBufferMessage
    );

    template<
        IsFlatBufferTable Table
    >
    ProtoValue(
        backing_store backingStore,
        const Table* flatBufferMessage
    )
        :
        ProtoValue(
            std::move(backingStore),
            flat_buffer_message{ reinterpret_cast<const flatbuffers::Table*>(flatBufferMessage) })
    {
    }

    ProtoValue(
        protocol_buffer_message protocolBufferMessage
    );

    ProtoValue(
        backing_store backingStore,
        protocol_buffer_message protocolBufferMessage
    );

    ProtoValue(
        const google::protobuf::Message* protocolBufferMessage
    );

    ProtoValue(
        backing_store backingStore,
        const google::protobuf::Message* protocolBufferMessage
    );

    template<
        IsNativeTable NativeTable
    > ProtoValue(
        const NativeTable& nativeFlatBufferMessage
    )
    {
        flatbuffers::FlatBufferBuilder builder;
        auto rootOffset = NativeTable::TableType::Pack(
            builder,
            &nativeFlatBufferMessage);
        builder.Finish(rootOffset);

        auto alignment = static_cast<uint8_t>(builder.GetBufferMinAlignment());
        auto detachedBuffer = std::make_shared<flatbuffers::DetachedBuffer>(
            builder.Release());
        auto span = std::span<const uint8_t>
        {
            detachedBuffer->data(),
            detachedBuffer->size()
        };

        AlignedMessage alignedMessageData
        {
            alignment,
            as_bytes(span),
        };

        message_data.emplace<flat_buffers_aligned_message_data>(
            AlignedMessageData
            {
                detachedBuffer,
                alignedMessageData
            });

        auto table = ::flatbuffers::GetRoot<flatbuffers::Table>(
            span.data());

        message.emplace<flat_buffers_table_pointer>(table);
    }

    static ProtoValue FlatBuffer(
        flat_buffer_message flatBufferBufferMessage
    );

    template<
        IsFlatBufferTable Table
    >
    static ProtoValue FlatBuffer(
        const Table* flatBufferMessage
    )
    {
        return FlatBuffer(
            flat_buffer_message{ reinterpret_cast<const flatbuffers::Table*>(flatBufferMessage) });
    }

    static ProtoValue FlatBuffer(
        backing_store backingStore,
        flat_buffer_message flatBufferBufferMessage = {}
    );

    template<
        IsFlatBufferTable Table
    >
    static ProtoValue FlatBuffer(
        backing_store backingStore,
        const Table* flatBufferMessage
    )
    {
        return FlatBuffer(
            std::move(backingStore),
            flat_buffer_message{ reinterpret_cast<const flatbuffers::Table*>(flatBufferMessage) });
    }

    static ProtoValue ProtocolBuffer(
        protocol_buffer_message protocolBufferMessage
    );

    static ProtoValue ProtocolBuffer(
        backing_store backingStore,
        protocol_buffer_message protocolBufferMessage = {}
    );

    ~ProtoValue();

    static ProtoValue KeyMin();
    static ProtoValue KeyMax();

    explicit operator bool() const;
    bool operator !() const;
    bool has_value() const;

    bool is_protocol_buffer() const;
    bool is_flat_buffer() const;

    const google::protobuf::Message* as_message_if() const;
    const flatbuffers::Table* as_table_if() const;

    template<
        IsProtocolBufferMessage TMessage
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

    template<
        IsFlatBufferTable Table
    > const Table* cast_if() const
    {
        return reinterpret_cast<const Table*>(as_table_if());
    }

    template<
        IsProtocolBufferMessage Message
    > ProtoValue& unpack()&
    {
        if (is_protocol_buffer()
            && !cast_if<Message>())
        {
            auto unpackedMessage = std::make_shared<Message>();
            unpack(&*unpackedMessage);
            message.emplace<protocol_buffers_message_shared_ptr>(
                std::move(unpackedMessage));
        }

        return *this;
    }
    
    template<
        IsProtocolBufferMessage Message
    > ProtoValue&& unpack()&&
    {
        return std::move(this->unpack<Message>());
    }

    void unpack(
        google::protobuf::Message* destination
    ) const;

    bool pack(
        std::string* destination
    ) const;

    ProtoValue& pack() &;
    ProtoValue&& pack() &&;

    ProtoValue pack_unowned() const;

    std::span<const std::byte> as_protocol_buffer_bytes_if() const;
    std::span<const std::byte> as_flat_buffer_bytes_if() const;
    AlignedMessage as_aligned_message_if() const;

    bool IsKeyMin() const;
    bool IsKeyMax() const;
};

template<
    IsFlatBufferTable Table
>
FlatMessage<Table>::operator ProtoValue() const & noexcept
{
    if (m_table)
    {
        return ProtoValue
        {
            m_storedMessage ?
                AlignedMessageData
                {
                    m_storedMessage,
                    m_storedMessage->Content,
                }
                :
                AlignedMessageData{},
            reinterpret_cast<const flatbuffers::Table*>(m_table),
        };
    }
    else
    {
        return {};
    }
}

template<
    IsFlatBufferTable Table
>
FlatMessage<Table>::operator ProtoValue() && noexcept
{
    if (m_table)
    {
        return ProtoValue
        {
            m_storedMessage ?
                AlignedMessageData
                {
                    std::move(m_storedMessage),
                    m_storedMessage->Content,
                }
                :
                AlignedMessageData{},
            reinterpret_cast<const flatbuffers::Table*>(m_table),
        };
    }
    else
    {
        return {};
    }
}

}