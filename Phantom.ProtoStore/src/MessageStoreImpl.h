#pragma once

#include "StandardTypes.h"
#include "ExtentName.h"
#include <cppcoro/async_mutex.hpp>
#include "MessageStore.h"
#include "Checksum.h"
#include "Phantom.System/async_reader_writer_lock.h"

namespace Phantom::ProtoStore
{

class RandomMessageReaderWriterBase
{
public:
    const shared_ptr<IChecksumAlgorithmFactory> m_checksumAlgorithmFactory;
    const FlatMessage<FlatBuffers::ExtentHeader> m_header;

    static ExtentOffset align(
        ExtentOffset base,
        uint8_t alignment
    );
    static bool is_aligned(
        ExtentOffset offset,
        uint8_t alignment
    );
    static bool is_contained_within(
        std::span<const byte> inner,
        std::span<const byte> outer
    );

    ExtentOffset to_underlying_extent_offset(
        ExtentOffset
    );

    static uint32_t checksum_v1(
        std::span<const byte>
    );

    RandomMessageReaderWriterBase(
        FlatMessage<FlatBuffers::ExtentHeader> header,
        shared_ptr<IChecksumAlgorithmFactory> checksumAlgorithmFactory);
};

class RandomMessageReader
    :
    private RandomMessageReaderWriterBase,
    public IRandomMessageReader
{
    const shared_ptr<IReadableExtent> m_extent;

public:
    RandomMessageReader(
        shared_ptr<IReadableExtent> extent,
        FlatMessage<FlatBuffers::ExtentHeader> header,
        shared_ptr<IChecksumAlgorithmFactory> checksumAlgorithmFactory);

    virtual task<DataReference<StoredMessage>> ReadFlatMessage(
        ExtentOffset extentOffset
    ) override;

    virtual task< DataReference<StoredMessage>> Read(
        ExtentOffset extentOffset,
        Message& message
    ) override;
};

class RandomMessageWriter
    :
    private RandomMessageReaderWriterBase,
    public IRandomMessageWriter
{
    const shared_ptr<IWritableExtent> m_extent;
    const uint8_t m_checksumSize;

    task<DataReference<StoredMessage>> WriteMessage(
        ExtentOffset extentOffset,
        std::span<const byte> messageV1Header,
        uint8_t messageAlignment,
        uint32_t messageSize,
        FlushBehavior flushBehavior,
        std::function<void(std::span<std::byte>)> fillMessageBuffer
    );

public:
    RandomMessageWriter(
        shared_ptr<IWritableExtent> extent,
        FlatMessage<FlatBuffers::ExtentHeader> header,
        shared_ptr<IChecksumAlgorithmFactory> checksumAlgorithmFactory);

    ExtentOffsetRange GetWriteRange(
        ExtentOffset extentOffset,
        uint8_t messageAlignment,
        uint32_t messageSize);

    virtual task<DataReference<StoredMessage>> WriteFlatMessage(
        ExtentOffset extentOffset,
        const StoredMessage& message,
        FlushBehavior flushBehavior
    ) override;

    virtual task<DataReference<StoredMessage>> Write(
        ExtentOffset extentOffset,
        const Message& message,
        FlushBehavior flushBehavior
    ) override;
};

class SequentialMessageReader
    :
    public ISequentialMessageReader
{
    const shared_ptr<RandomMessageReader> m_randomMessageReader;
    ExtentOffset m_currentOffset;
public:
    SequentialMessageReader(
        shared_ptr<RandomMessageReader> randomMessageReader);

    virtual task<DataReference<StoredMessage>> ReadFlatMessage(
    ) override;

    virtual task<DataReference<StoredMessage>> Read(
        Message& message
    ) override;
};

class SequentialMessageWriter
    :
    public ISequentialMessageWriter
{
    const shared_ptr<RandomMessageWriter> m_randomMessageWriter;
    std::atomic<ExtentOffset> m_currentOffset;
public:
    SequentialMessageWriter(
        shared_ptr<RandomMessageWriter> randomMessageWriter);

    virtual task<DataReference<StoredMessage>> WriteFlatMessage(
        const StoredMessage& flatMessage,
        FlushBehavior flushBehavior
    ) override;

    virtual task< DataReference<StoredMessage>> Write(
        const Message& message, 
        FlushBehavior flushBehavior
    ) override;

    virtual task<ExtentOffset> CurrentOffset(
    ) override;
};

class MessageStore
    :
    public IMessageStore
{
    Schedulers m_schedulers;
    const shared_ptr<IExtentStore> m_extentStore;
    async_reader_writer_lock m_extentsLock;
    std::unordered_map<ExtentName, shared_ptr<RandomMessageReader>> m_readableExtents;
    shared_ptr<IChecksumAlgorithmFactory> m_checksumAlgorithmFactory;

    task<shared_ptr<RandomMessageReader>> OpenExtentForRandomReadAccessImpl(
        shared_ptr<IReadableExtent> readableExtent);

    task<shared_ptr<RandomMessageReader>> OpenExtentForRandomReadAccessImpl(
        const ExtentName& ExtentName);

    task<shared_ptr<RandomMessageWriter>> OpenExtentForRandomWriteAccessImpl(
        ExtentName ExtentName);

public:
    MessageStore(
        Schedulers schedulers,
        shared_ptr<IExtentStore> extentStore);

    // Inherited via IMessageStore
    virtual task<shared_ptr<IRandomMessageReader>> OpenExtentForRandomReadAccess(
        const shared_ptr<IReadableExtent>& readableExtent
    ) override;

    virtual task<shared_ptr<IRandomMessageReader>> OpenExtentForRandomReadAccess(
        ExtentName extentName
    ) override;

    virtual task<shared_ptr<IRandomMessageWriter>> OpenExtentForRandomWriteAccess(
        ExtentName extentName
    ) override;

    virtual task<shared_ptr<ISequentialMessageReader>> OpenExtentForSequentialReadAccess(
        const shared_ptr<IReadableExtent>& readableExtent
    ) override;

    virtual task<shared_ptr<ISequentialMessageReader>> OpenExtentForSequentialReadAccess(
        ExtentName extentName
    ) override;

    virtual task<shared_ptr<ISequentialMessageWriter>> OpenExtentForSequentialWriteAccess(
        ExtentName extentName
    ) override;
};
}
