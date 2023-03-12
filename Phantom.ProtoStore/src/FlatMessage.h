#include "Phantom.ProtoStore/Payloads.h"
#include <flatbuffers/flatbuffers.h>
#include "Phantom.System/concepts.h"

namespace Phantom::ProtoStore
{

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

    operator ProtoValue() const& noexcept;
    operator ProtoValue() && noexcept;

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


template<
    IsFlatBufferTable Table
>
FlatMessage<Table>::operator ProtoValue() const& noexcept
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