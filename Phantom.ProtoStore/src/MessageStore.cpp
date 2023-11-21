#include "Checksum.h"
#include "ExtentName.h"
#include "ExtentStore.h"
#include "MessageStoreImpl.h"
#include "Phantom.ProtoStore/numeric_cast.h"
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"
#include "StandardTypes.h"
#include <cppcoro/async_mutex.hpp>

namespace Phantom::ProtoStore
{
using google::protobuf::io::CodedInputStream;
using google::protobuf::io::CodedOutputStream;

RandomMessageReaderWriterBase::RandomMessageReaderWriterBase(
    FlatMessage<FlatBuffers::ExtentHeader> header
)
    :
    m_header(std::move(header))
{}

ExtentOffset RandomMessageReaderWriterBase::align(
    ExtentOffset base,
    uint8_t alignment
)
{
    ExtentOffset alignment64 = alignment;

    assert(
        alignment == 1
        || alignment == 2
        || alignment == 4
        || alignment == 8
        || alignment == 16
        || alignment == 32);

    return (base + (alignment64 - 1)) & ~(alignment64 - 1);
}

bool RandomMessageReaderWriterBase::is_aligned(
    ExtentOffset offset,
    uint8_t alignment
)
{
    assert(
        alignment == 1
        || alignment == 2
        || alignment == 4
        || alignment == 8
        || alignment == 16
        || alignment == 32);

    return (offset & ~(alignment - 1)) == offset;
}

bool RandomMessageReaderWriterBase::is_contained_within(
    std::span<const byte> inner,
    std::span<const byte> outer
)
{
    std::less_equal<const byte*> less_equal;
    return less_equal(outer.data(), inner.data())
        && less_equal(inner.data() + inner.size(), outer.data() + outer.size());
}

uint32_t RandomMessageReaderWriterBase::to_message_size_and_alignment(
    uint32_t messageSize,
    uint8_t alignment
)
{
    uint32_t truncatedMessageSize = messageSize & sizeMask;
    if (truncatedMessageSize != messageSize)
    {
        throw std::range_error("messageSize");
    }
    assert(
        alignment == 1
        || alignment == 2
        || alignment == 4
        || alignment == 8
        || alignment == 16
        || alignment == 32
        );
    // Round alignment up to 4.
    alignment = std::max<uint8_t>(alignment, 4);

    uint32_t messageAlignmentBits = (std::bit_width<uint8_t>(alignment - 1) - 2) << 30;

    return truncatedMessageSize | messageAlignmentBits;
}

uint32_t RandomMessageReaderWriterBase::from_message_size_and_alignment_to_size(
    uint32_t message_size_and_alignment)
{
    return message_size_and_alignment & sizeMask;

}

uint8_t RandomMessageReaderWriterBase::from_message_size_and_alignment_to_alignment(
    uint32_t message_size_and_alignment)
{
    return 1 << (((message_size_and_alignment & alignmentMask) >> 30) + 2);
}

ExtentOffset RandomMessageReaderWriterBase::to_underlying_extent_offset(
    ExtentOffset extentOffset
)
{
    return align(
        extentOffset + m_header.data().Content.Payload.size(),
        4
    );
}

RandomMessageReader::RandomMessageReader(
    shared_ptr<IReadableExtent> extent,
    FlatMessage<FlatBuffers::ExtentHeader> header)
    :
    RandomMessageReaderWriterBase
    {
        std::move(header),
    },
    m_extent(std::move(extent))
{}

task<DataReference<StoredMessage>> RandomMessageReader::Read(
    const FlatBuffers::MessageReference_V1* location
)
{
    static_assert(alignof(FlatBuffers::MessageHeader_V1) == 4);

    // A zero length message has a non-zero CRC,
    // so reading a zero length message with a zero CRC
    // indicates we're reading out of bounds.
    if (location->message_header().crc32() == 0
        && location->message_header().message_size_and_alignment() == 0)
    {
        co_return{};
    }

    // A MessageHeader_V1 has a specific alignment of 4.
    assert(is_aligned(location->message_offset(), 4));
    auto headerExtentOffset = to_underlying_extent_offset(location->message_offset());

    auto messageHeader = &location->message_header();

    auto messageSize = from_message_size_and_alignment_to_size(
        messageHeader->message_size_and_alignment());
    auto messageAlignment = from_message_size_and_alignment_to_alignment(
        messageHeader->message_size_and_alignment());

    auto messageExtentOffset = align(
        headerExtentOffset + sizeof(FlatBuffers::MessageHeader_V1),
        messageAlignment);

    auto headerSpaceSize = messageExtentOffset - headerExtentOffset;

    auto envelopeReadBuffer = co_await m_extent->Read(
        headerExtentOffset,
        headerSpaceSize + messageSize
    );

    StoredMessage storedMessage
    {
        .ExtentFormatVersion = m_header->extent_version(),
        .Header = 
        { 
            alignof(MessageHeader_V1), 
            envelopeReadBuffer->subspan(0, sizeof(FlatBuffers::MessageHeader_V1))
        },
        .Content =
        {
            messageAlignment,
            envelopeReadBuffer->subspan(headerSpaceSize),
        },
        .DataRange = 
        {
            .Beginning = location->message_offset(),
            .End = align(messageExtentOffset - headerExtentOffset + location->message_offset() + messageSize, 4),
        },
    };

    co_return DataReference
    {
        std::move(envelopeReadBuffer),
        storedMessage
    };
}

task<DataReference<StoredMessage>> RandomMessageReader::Read(
    ExtentOffset extentOffset
)
{
    static_assert(alignof(FlatBuffers::MessageHeader_V1) == 4);

    // A MessageHeader_V1 has a specific alignment of 4.
    assert(is_aligned(extentOffset, 4));
    auto headerExtentOffset = to_underlying_extent_offset(extentOffset);

    auto messageHeaderReadBuffer = co_await m_extent->Read(
        headerExtentOffset,
        sizeof(FlatBuffers::MessageHeader_V1));

    if (!messageHeaderReadBuffer->data())
    {
        co_return{};
    }

    auto messageHeader = reinterpret_cast<const FlatBuffers::MessageHeader_V1*>(
        messageHeaderReadBuffer->data());

    FlatBuffers::MessageReference_V1 messageReference(
        *messageHeader,
        extentOffset
    );

    co_return co_await Read(
        &messageReference);
}

RandomMessageWriter::RandomMessageWriter(
    shared_ptr<IWritableExtent> extent,
    FlatMessage<FlatBuffers::ExtentHeader> header)
    :
    RandomMessageReaderWriterBase
    {
        std::move(header)
    },
    m_extent(std::move(extent)),
    m_checksumSize()
{
}

task<DataReference<StoredMessage>> RandomMessageWriter::Write(
    ExtentOffset extentOffset,
    const MessageHeader_V1* messageV1Header,
    uint8_t messageAlignment,
    uint32_t messageSize,
    FlushBehavior flushBehavior,
    std::function<void(std::span<std::byte>)> fillMessageBuffer
)
{
    static_assert(alignof(FlatBuffers::MessageHeader_V1) == 4);

    // A MessageHeader_V1 has a specific alignment of 4.
    assert(is_aligned(extentOffset, 4));
    auto headerExtentOffset = to_underlying_extent_offset(extentOffset);

    messageAlignment = std::max<uint8_t>(4, messageAlignment);

    auto messageExtentOffset = align(
        headerExtentOffset + sizeof(FlatBuffers::MessageHeader_V1),
        messageAlignment);

    auto messageSizeAndAlignment = to_message_size_and_alignment(
        messageSize,
        messageAlignment);

    auto envelopeSize = messageExtentOffset + messageSize - headerExtentOffset;

    auto writeBuffer = co_await m_extent->CreateWriteBuffer();
    auto writeBufferData = co_await writeBuffer->Write(
        headerExtentOffset,
        envelopeSize
    );

    std::span<byte> headerWriteBuffer = writeBufferData->subspan(
        0,
        sizeof(FlatBuffers::MessageHeader_V1));

    std::span<byte> messageWriteBuffer = writeBufferData->subspan(
        messageExtentOffset - headerExtentOffset);

    // Copy the actual message in now, so we can CRC it if necessary.
    fillMessageBuffer(
        messageWriteBuffer);

    auto* messageHeader = reinterpret_cast<FlatBuffers::MessageHeader_V1*>(
        headerWriteBuffer.data());

    if (messageV1Header)
    {
        // The incoming message has an already-computed header of the correct format,
        // We can copy the header.
        *messageHeader = *messageV1Header;
    }
    else
    {
        // We need to actually compute the CRC and message size.
        messageHeader->mutate_crc32(
            // Note that the CRC includes the trailing 0 bytes,
            // so we don't CRC the incoming message,
            // but the message with rounded up byte count.
            checksum_v1(messageWriteBuffer)
        );

        messageHeader->mutate_message_size_and_alignment(
            messageSizeAndAlignment);
    }

    StoredMessage storedMessage
    {
        .ExtentFormatVersion = FlatBuffers::ExtentFormatVersion::V1,
        .Header =
        {
            alignof(MessageHeader_V1),
            headerWriteBuffer
        },
        .Content =
        {
            messageAlignment,
            messageWriteBuffer
        },
        .DataRange = { extentOffset, align(messageExtentOffset - headerExtentOffset + extentOffset + messageSize, 4) },
    };

    co_await writeBuffer->Commit();
    if (flushBehavior == FlushBehavior::Flush)
    {
        co_await writeBuffer->Flush();
    }

    co_return DataReference<StoredMessage>
    {
        std::move(writeBufferData),
        storedMessage
    };
}

ExtentOffsetRange RandomMessageWriter::GetWriteRange(
    ExtentOffset extentOffset,
    uint8_t messageAlignment,
    uint32_t messageSize)
{
    static_assert(alignof(FlatBuffers::MessageHeader_V1) == 4);

    // A MessageHeader_V1 has a specific alignment of 4.
    assert(is_aligned(extentOffset, 4));
    auto headerExtentOffset = to_underlying_extent_offset(extentOffset);

    messageAlignment = std::max<uint8_t>(4, messageAlignment);

    auto messageExtentOffset = align(
        headerExtentOffset + sizeof(FlatBuffers::MessageHeader_V1),
        messageAlignment);

    // Our message size / alignment algorithm rounds up the size to 4 bytes.
    auto alignedMessageSize = align(
        messageSize,
        4);

    uint32_t alignedMessageSize32 = numeric_cast(alignedMessageSize);

    return
    {
        extentOffset,
        messageExtentOffset - headerExtentOffset + extentOffset + alignedMessageSize32
    };
}

task<DataReference<StoredMessage>> RandomMessageWriter::Write(
    ExtentOffset extentOffset,
    const StoredMessage& message,
    FlushBehavior flushBehavior
)
{
    return Write(
        extentOffset,
        message.Header_V1(),
        message.Content.Alignment,
        numeric_cast(message.Content.Payload.size()),
        flushBehavior,
        [&](std::span<std::byte> messageWriteBuffer)
    {
        std::copy_n(
            message.Content.Payload.data(),
            message.Content.Payload.size(),
            messageWriteBuffer.data()
        );
    });
}

SequentialMessageReader::SequentialMessageReader(
    shared_ptr<RandomMessageReader> randomMessageReader
)
    : m_randomMessageReader(randomMessageReader),
    m_currentOffset(0)
{
}

task<DataReference<StoredMessage>> SequentialMessageReader::Read(
)
{
    if (!m_randomMessageReader)
    {
        co_return{};
    }

    auto storedMessage = co_await m_randomMessageReader->Read(
        m_currentOffset);
    if (!storedMessage)
    {
        co_return{};
    }
    m_currentOffset = storedMessage->DataRange.End;
    co_return std::move(storedMessage);
}

SequentialMessageWriter::SequentialMessageWriter(
    shared_ptr<RandomMessageWriter> randomMessageWriter
)
    : 
    m_randomMessageWriter(randomMessageWriter),
    m_currentOffset(0)
{}

task<DataReference<StoredMessage>> SequentialMessageWriter::Write(
    const StoredMessage& flatMessage,
    FlushBehavior flushBehavior
)
{
    auto writeOffset = m_currentOffset.load(
        std::memory_order_relaxed);
    
    ExtentOffsetRange writeRange;
    do
    {
        writeRange = m_randomMessageWriter->GetWriteRange(
            writeOffset,
            flatMessage.Content.Alignment,
            numeric_cast(flatMessage.Content.Payload.size()));
    } while (!m_currentOffset.compare_exchange_weak(
        writeOffset,
        writeRange.End,
        std::memory_order_relaxed
    ));

    co_return co_await m_randomMessageWriter->Write(
        writeOffset,
        flatMessage,
        flushBehavior);
}

task<ExtentOffset> SequentialMessageWriter::CurrentOffset(
)
{
    co_return m_currentOffset.load(
        std::memory_order_acquire);
}

task<shared_ptr<RandomMessageReader>> MessageStore::OpenExtentForRandomReadAccessImpl(
    shared_ptr<IReadableExtent> readableExtent)
{
    auto extentHeaderSizeBuffer = co_await readableExtent->Read(
        0,
        sizeof(flatbuffers::uoffset_t));

    if (!extentHeaderSizeBuffer->data())
    {
        co_return{};
    }

    auto extentHeaderSize = *reinterpret_cast<const flatbuffers::uoffset_t*>(extentHeaderSizeBuffer->data());
    auto envelopeBuffer = co_await readableExtent->Read(
        0,
        sizeof(flatbuffers::uoffset_t) + extentHeaderSize
    );

    flatbuffers::Verifier headerVerifier(
        reinterpret_cast<const uint8_t*>(envelopeBuffer->data()),
        envelopeBuffer->size());
    if (!headerVerifier.VerifySizePrefixedBuffer<FlatBuffers::ExtentHeader>("PSEX"))
    {
        throw std::range_error("Invalid extent.");
    }

    auto header = flatbuffers::GetSizePrefixedRoot<FlatBuffers::ExtentHeader>(
        envelopeBuffer->data()
        );

    StoredMessage storedHeader
    {
        .ExtentFormatVersion = header->extent_version(),
        .Content = 
        {
            32,
            envelopeBuffer.data(),
        },
    };

    auto headerFlatMessage = FlatMessage<FlatBuffers::ExtentHeader>
    {
        DataReference<StoredMessage>
        {
            std::move(envelopeBuffer),
            std::move(storedHeader),
        },
        header
    };

    co_return make_shared<RandomMessageReader>(
        std::move(readableExtent),
        std::move(headerFlatMessage));
}

task<shared_ptr<RandomMessageReader>> MessageStore::OpenExtentForRandomReadAccessImpl(
    const ExtentName* extentName)
{
    shared_ptr<RandomMessageReader> reader;

    co_await execute_conditional_read_unlikely_write_operation(
        m_extentsLock,
        *m_schedulers.LockScheduler,
        [&](auto hasWriteLock) -> task<bool>
    {
        if (m_readableExtents.contains(FlatValue{ extentName }))
        {
            reader = m_readableExtents[FlatValue{ extentName }];
        }
        co_return reader != nullptr;
    },
        [&]() -> task<>
    {
        reader = co_await OpenExtentForRandomReadAccessImpl(
            co_await m_extentStore->OpenExtentForRead(
                extentName));

        m_readableExtents[Clone(extentName)]
            = reader;
    });

    co_return reader;
}

MessageStore::MessageStore(
    Schedulers schedulers,
    shared_ptr<IExtentStore> extentStore)
    :
    m_schedulers(schedulers),
    m_extentStore(move(extentStore)),
    m_readableExtents(
        0,
        FlatBuffersSchemas::ExtentName_Comparers.hash,
        FlatBuffersSchemas::ExtentName_Comparers.equal_to
    )
{
}

// Inherited via IMessageStore
task<shared_ptr<IRandomMessageReader>> MessageStore::OpenExtentForRandomReadAccess(
    const shared_ptr<IReadableExtent>& readableExtent
)
{
    co_return co_await OpenExtentForRandomReadAccessImpl(
        readableExtent);
}

task<shared_ptr<IRandomMessageReader>> MessageStore::OpenExtentForRandomReadAccess(
    const FlatBuffers::ExtentName* extentName
)
{
    co_return co_await OpenExtentForRandomReadAccessImpl(
        extentName);
}

task<shared_ptr<IRandomMessageWriter>> MessageStore::OpenExtentForRandomWriteAccess(
    const FlatBuffers::ExtentName* extentName
)
{
    co_return co_await OpenExtentForRandomWriteAccessImpl(extentName);
}
    
task<shared_ptr<RandomMessageWriter>> MessageStore::OpenExtentForRandomWriteAccessImpl(
    const FlatBuffers::ExtentName* extentName
)
{
    // Remove the existing cached readable extent
    // and ensure nobody opens it until we've finished writing the header.
    auto lock = co_await m_extentsLock.writer().scoped_lock_async();
    m_readableExtents.erase(FlatValue{ extentName });
    auto writableExtent = co_await m_extentStore->OpenExtentForWrite(
        extentName);

    flatbuffers::FlatBufferBuilder headerBuilder;
    auto headerOffset = FlatBuffers::CreateExtentHeader(
        headerBuilder,
        FlatBuffers::ExtentFormatVersion::V1
    );
    headerBuilder.FinishSizePrefixed(
        headerOffset,
        "PSEX"
    );
        
    auto headerWriteBuffer = co_await writableExtent->CreateWriteBuffer();
    auto headerWritableRawData = co_await headerWriteBuffer->Write(
        0,
        headerBuilder.GetSize()
    );
    std::copy_n(
        reinterpret_cast<std::byte*>(headerBuilder.GetBufferPointer()),
        headerBuilder.GetSize(),
        headerWritableRawData.data().data()
    );
    co_await headerWriteBuffer->Flush();
    lock.unlock();

    StoredMessage storedExtentHeader
    {
        .ExtentFormatVersion = FlatBuffers::ExtentFormatVersion::V1,
        .Header = {},
        .Content =
        {
            static_cast<uint8_t>(headerBuilder.GetBufferMinAlignment()),
            headerWritableRawData.data(),
        },
    };
    
    auto* extentHeader = flatbuffers::GetSizePrefixedRoot<FlatBuffers::ExtentHeader>(
        storedExtentHeader.Content.Payload.data());

    FlatMessage<FlatBuffers::ExtentHeader> flatMessage(
        DataReference<StoredMessage>
        {
            std::move(headerWritableRawData),
            storedExtentHeader
        },
        extentHeader);

    co_return make_shared<RandomMessageWriter>(
        std::move(writableExtent),
        std::move(flatMessage));
}

task<shared_ptr<ISequentialMessageReader>> MessageStore::OpenExtentForSequentialReadAccess(
    const shared_ptr<IReadableExtent>& readableExtent
)
{
    co_return make_shared<SequentialMessageReader>(
        co_await OpenExtentForRandomReadAccessImpl(
            readableExtent));
}

task<shared_ptr<ISequentialMessageReader>> MessageStore::OpenExtentForSequentialReadAccess(
    const FlatBuffers::ExtentName* extentName
)
{
    co_return co_await OpenExtentForSequentialReadAccess(
        co_await m_extentStore->OpenExtentForRead(
            extentName));
}

task<shared_ptr<ISequentialMessageWriter>> MessageStore::OpenExtentForSequentialWriteAccess(
    const FlatBuffers::ExtentName* extentName
) 
{
    co_return make_shared<SequentialMessageWriter>(
        co_await OpenExtentForRandomWriteAccessImpl(
            extentName));
}

shared_ptr<IMessageStore> MakeMessageStore(
    Schedulers schedulers,
    shared_ptr<IExtentStore> extentStore)
{
    return make_shared<MessageStore>(
        schedulers,
        move(extentStore));
}
}
