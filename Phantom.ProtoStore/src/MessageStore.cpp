#include "StandardTypes.h"
#include "Checksum.h"
#include "MessageStoreImpl.h"
#include "ExtentStore.h"
#include <cppcoro/async_mutex.hpp>
#include "ExtentName.h"
#include "src/ProtoStoreInternal_generated.h"
#include <boost/crc.hpp>

namespace Phantom::ProtoStore
{
using google::protobuf::io::CodedInputStream;
using google::protobuf::io::CodedOutputStream;

RandomMessageReaderWriterBase::RandomMessageReaderWriterBase(
    FlatMessage<FlatBuffers::ExtentHeader> header,
    shared_ptr<IChecksumAlgorithmFactory> checksumAlgorithmFactory
)
    :
    m_header(std::move(header)),
    m_checksumAlgorithmFactory(std::move(checksumAlgorithmFactory))
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

uint32_t RandomMessageReaderWriterBase::checksum_v1(
    std::span<const byte> data
)
{
    using crc_v1_type = boost::crc_optimal<32, 0x1EDC6F41, 0xffffffff, 0xffffffff, true, true>;
    crc_v1_type crc;
    crc.process_bytes(
        data.data(),
        data.size()
    );
    return crc.checksum();
}

ExtentOffset RandomMessageReaderWriterBase::to_underlying_extent_offset(
    ExtentOffset extentOffset
)
{
    return align(
        extentOffset + m_header.data().Message.size(),
        4
    );
}

RandomMessageReader::RandomMessageReader(
    shared_ptr<IReadableExtent> extent,
    FlatMessage<FlatBuffers::ExtentHeader> header,
    shared_ptr<IChecksumAlgorithmFactory> checksumAlgorithmFactory)
    :
    RandomMessageReaderWriterBase
    {
        std::move(header),
        std::move(checksumAlgorithmFactory)
    },
    m_extent(std::move(extent))
{}

task<DataReference<StoredMessage>> RandomMessageReader::ReadFlatMessage(
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

    auto messageHeader = reinterpret_cast<const FlatBuffers::MessageHeader_V1*>(
        messageHeaderReadBuffer->data());

    auto messageSize = messageHeader->message_size_and_alignment() & ~uint32_t(0x3);
    uint8_t messageAlignment = 1 << ((messageHeader->message_size_and_alignment() & 0x3) + 2);

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
        .MessageAlignment = messageAlignment,
        .Message = envelopeReadBuffer->subspan(headerSpaceSize),
        .Header = envelopeReadBuffer->subspan(0, sizeof(FlatBuffers::MessageHeader_V1)),
        .DataRange = 
        {
            .Beginning = extentOffset,
            .End = messageExtentOffset - headerExtentOffset + extentOffset + messageSize,
        },
    };

    auto crc = checksum_v1(storedMessage.Message);
    if (crc != messageHeader->crc32())
    {
        throw std::range_error("crc");
    }

    co_return DataReference
    {
        std::move(envelopeReadBuffer),
        storedMessage
    };
}

task<DataReference<StoredMessage>> RandomMessageReader::Read(
    ExtentOffset extentOffset,
    Message& message
)
{
    auto flatMessage = co_await ReadFlatMessage(extentOffset);

    google::protobuf::io::ArrayInputStream messageStream(
        flatMessage->Message.data(),
        flatMessage->Message.size()
    );

    message.ParseFromZeroCopyStream(
        &messageStream);

    co_return std::move(flatMessage);
}

RandomMessageWriter::RandomMessageWriter(
    shared_ptr<IWritableExtent> extent,
    FlatMessage<FlatBuffers::ExtentHeader> header,
    shared_ptr<IChecksumAlgorithmFactory> checksumAlgorithmFactory)
    :
    RandomMessageReaderWriterBase
    {
        std::move(header),
        std::move(checksumAlgorithmFactory)
    },
    m_extent(std::move(extent)),
    m_checksumSize(m_checksumAlgorithmFactory->Create()->SizeInBytes())
{
}

task<DataReference<StoredMessage>> RandomMessageWriter::WriteMessage(
    ExtentOffset extentOffset,
    std::span<const byte> messageV1Header,
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

    // Our message size / alignment algorithm rounds up the size to 4 bytes.
    auto alignedMessageSize = align(
        messageSize,
        4);

    uint32_t alignedMessageSize32 = alignedMessageSize;
    if (alignedMessageSize32 != alignedMessageSize)
    {
        throw std::range_error("message.Message.size()");
    }

    auto envelopeSize = messageExtentOffset + alignedMessageSize - headerExtentOffset;

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

    if (messageV1Header.data())
    {
        // The incoming message has an already-computed header of the correct format,
        // We can copy the header.
        *messageHeader = *reinterpret_cast<const FlatBuffers::MessageHeader_V1*>(messageV1Header.data());
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

        uint32_t messageAlignmentBits = std::bit_width<uint8_t>(messageAlignment - 1) - 2;
        assert(messageAlignmentBits >= 0 && messageAlignmentBits <= 3);

        messageHeader->mutate_message_size_and_alignment(
            alignedMessageSize32 | messageAlignmentBits
        );
    }

    StoredMessage storedMessage
    {
        .ExtentFormatVersion = FlatBuffers::ExtentFormatVersion::V1,
        .MessageAlignment = messageAlignment,
        .Message = messageWriteBuffer,
        .Header = headerWriteBuffer,
        .DataRange = { extentOffset, messageExtentOffset - headerExtentOffset + extentOffset + alignedMessageSize32 },
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

    uint32_t alignedMessageSize32 = alignedMessageSize;
    if (alignedMessageSize32 != alignedMessageSize)
    {
        throw std::range_error("message.Message.size()");
    }

    return
    {
        extentOffset,
        messageExtentOffset - headerExtentOffset + extentOffset + alignedMessageSize32
    };
}

task<DataReference<StoredMessage>> RandomMessageWriter::WriteFlatMessage(
    ExtentOffset extentOffset,
    const StoredMessage& message,
    FlushBehavior flushBehavior
)
{
    return WriteMessage(
        extentOffset,
        message.Header,
        message.MessageAlignment,
        message.Message.size(),
        flushBehavior,
        [&](std::span<std::byte> messageWriteBuffer)
    {
        std::copy_n(
            message.Message.data(),
            message.Message.size(),
            messageWriteBuffer.data()
        );
    });
}

task< DataReference<StoredMessage>> RandomMessageWriter::Write(
    ExtentOffset extentOffset,
    const Message& message,
    FlushBehavior flushBehavior
)
{
    co_return co_await WriteMessage(
        extentOffset,
        {},
        1,
        message.ByteSizeLong(),
        flushBehavior,
        [&](std::span<std::byte> messageWriteBuffer)
    {
        google::protobuf::io::ArrayOutputStream stream(
            messageWriteBuffer.data(),
            messageWriteBuffer.size());

        message.SerializeToZeroCopyStream(&stream);
    });
}

SequentialMessageReader::SequentialMessageReader(
    shared_ptr<RandomMessageReader> randomMessageReader
)
    : m_randomMessageReader(randomMessageReader),
    m_currentOffset(0)
{
}

task<DataReference<StoredMessage>> SequentialMessageReader::ReadFlatMessage(
)
{
    auto storedMessage = co_await m_randomMessageReader->ReadFlatMessage(
        m_currentOffset);
    m_currentOffset = storedMessage->DataRange.End;
    co_return std::move(storedMessage);
}

task<DataReference<StoredMessage>> SequentialMessageReader::Read(
    Message& message
)
{
    auto readResult = co_await ReadFlatMessage();
    google::protobuf::io::ArrayInputStream stream(
        readResult->Message.data(),
        readResult->Message.size()
    );
    message.ParseFromZeroCopyStream(
        &stream);

    co_return std::move(readResult);
}

SequentialMessageWriter::SequentialMessageWriter(
    shared_ptr<RandomMessageWriter> randomMessageWriter
)
    : 
    m_randomMessageWriter(randomMessageWriter),
    m_currentOffset(0)
{}

task<DataReference<StoredMessage>> SequentialMessageWriter::WriteFlatMessage(
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
            flatMessage.MessageAlignment,
            flatMessage.Message.size());
    } while (!m_currentOffset.compare_exchange_weak(
        writeOffset,
        writeRange.End,
        std::memory_order_relaxed
    ));

    co_return co_await m_randomMessageWriter->WriteFlatMessage(
        writeOffset,
        flatMessage,
        flushBehavior);
}

task<DataReference<StoredMessage>> SequentialMessageWriter::Write(
    const Message& message,
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
            1,
            message.ByteSizeLong());
    } while (!m_currentOffset.compare_exchange_weak(
        writeOffset,
        writeRange.End,
        std::memory_order_relaxed
    ));

    co_return co_await m_randomMessageWriter->Write(
        writeOffset,
        message,
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
        .Message = envelopeBuffer.data(),
    };

    co_return make_shared<RandomMessageReader>(
        std::move(readableExtent),
        FlatMessage<FlatBuffers::ExtentHeader>
        {
            DataReference<StoredMessage>
            {
                std::move(envelopeBuffer),
                std::move(storedHeader),
            }
        },
        m_checksumAlgorithmFactory);
}

task<shared_ptr<RandomMessageReader>> MessageStore::OpenExtentForRandomReadAccessImpl(
    const ExtentName& extentName)
{
    shared_ptr<RandomMessageReader> reader;

    co_await execute_conditional_read_unlikely_write_operation(
        m_extentsLock,
        *m_schedulers.LockScheduler,
        [&](auto hasWriteLock) -> task<bool>
    {
        reader = m_readableExtents[extentName];
        co_return reader != nullptr;
    },
        [&]() -> task<>
    {
        reader = co_await OpenExtentForRandomReadAccessImpl(
            co_await m_extentStore->OpenExtentForRead(
                extentName));

        m_readableExtents[extentName]
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
    m_checksumAlgorithmFactory(MakeChecksumAlgorithmFactory())
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
    ExtentName extentName
)
{
    co_return co_await OpenExtentForRandomReadAccessImpl(
        extentName);
}

task<shared_ptr<IRandomMessageWriter>> MessageStore::OpenExtentForRandomWriteAccess(
    ExtentName extentName
)
{
    co_return co_await OpenExtentForRandomWriteAccessImpl(extentName);
}
    
task<shared_ptr<RandomMessageWriter>> MessageStore::OpenExtentForRandomWriteAccessImpl(
    ExtentName extentName
)
{
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

    StoredMessage storedHeader
    {
        .ExtentFormatVersion = FlatBuffers::ExtentFormatVersion::V1,
        .MessageAlignment = static_cast<uint8_t>(headerBuilder.GetBufferMinAlignment()),
        .Message = headerWritableRawData.data(),
        .Header = {},
    };
    auto* header = flatbuffers::GetSizePrefixedRoot<FlatBuffers::ExtentHeader>(
        storedHeader.Message.data());

    FlatMessage<FlatBuffers::ExtentHeader> flatMessage(
        DataReference<StoredMessage>
        {
            std::move(headerWritableRawData),
            storedHeader
        });

    co_return make_shared<RandomMessageWriter>(
        std::move(writableExtent),
        std::move(flatMessage),
        m_checksumAlgorithmFactory);
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
    ExtentName extentName
)
{
    co_return co_await OpenExtentForSequentialReadAccess(
        co_await m_extentStore->OpenExtentForRead(
            extentName));
}

task<shared_ptr<ISequentialMessageWriter>> MessageStore::OpenExtentForSequentialWriteAccess(
    ExtentName extentName
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
