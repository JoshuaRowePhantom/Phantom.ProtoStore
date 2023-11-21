#pragma once

#include "boost/unordered/concurrent_flat_map.hpp"
#include "ExtentName.h"
#include "MessageStore.h"
#include "Phantom.System/async_reader_writer_lock.h"
#include "Phantom.System/batching_worker.h"
#include "Phantom.ProtoStore/ProtoStoreInternal_generated.h"
#include "StandardTypes.h"
#include <cppcoro/async_mutex.hpp>

namespace Phantom::ProtoStore
{

class RandomMessageReaderWriterBase
    :
    public SerializationTypes
{
public:
    static const uint32_t alignmentMask = 0xc0000000;
    static const uint32_t sizeMask = ~alignmentMask;
    static const uint32_t alignmentShift = 30;

    const FlatMessage<FlatBuffers::ExtentHeader> m_header;

    static uint32_t to_message_size_and_alignment(
        uint32_t messageSize,
        uint8_t alignment);
    
    static uint32_t from_message_size_and_alignment_to_size(
        uint32_t message_size_and_alignment);
    
    static uint8_t from_message_size_and_alignment_to_alignment(
        uint32_t message_size_and_alignment);

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
    
    RandomMessageReaderWriterBase(
        FlatMessage<FlatBuffers::ExtentHeader> header);
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
        FlatMessage<FlatBuffers::ExtentHeader> header);

    virtual task<DataReference<StoredMessage>> Read(
        ExtentOffset extentOffset
    ) override;

    virtual task<DataReference<StoredMessage>> Read(
        const FlatBuffers::MessageReference_V1* location
    ) override;
};

class RandomMessageWriter
    :
    private RandomMessageReaderWriterBase,
    public IRandomMessageWriter
{
    const shared_ptr<IWritableExtent> m_extent;
    const uint8_t m_checksumSize = 4;

    task<DataReference<StoredMessage>> Write(
        ExtentOffset extentOffset,
        const MessageHeader_V1* messageV1Header,
        uint8_t messageAlignment,
        uint32_t messageSize,
        FlushBehavior flushBehavior,
        std::function<void(std::span<std::byte>)> fillMessageBuffer
    );

public:
    RandomMessageWriter(
        shared_ptr<IWritableExtent> extent,
        FlatMessage<FlatBuffers::ExtentHeader> header);

    ExtentOffsetRange GetWriteRange(
        ExtentOffset extentOffset,
        uint8_t messageAlignment,
        uint32_t messageSize);

    virtual task<DataReference<StoredMessage>> Write(
        ExtentOffset extentOffset,
        const StoredMessage& message,
        FlushBehavior flushBehavior
    ) override;

    virtual task<> Flush(
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

    virtual task<DataReference<StoredMessage>> Read(
    ) override;
};

class SequentialMessageWriter
    :
    public ISequentialMessageWriter
{
    const shared_ptr<RandomMessageWriter> m_randomMessageWriter;
    std::atomic<ExtentOffset> m_currentOffset;

    using incomplete_write_type = shared_task<DataReference<StoredMessage>>;
    using incomplete_writes_map_type = boost::concurrent_flat_map<void*, incomplete_write_type>;
    incomplete_writes_map_type m_incompleteWrites;
    std::vector<incomplete_write_type> m_incompleteWritesVector;
    Phantom::Coroutines::batching_worker<> m_flushWorker;

    shared_task<DataReference<StoredMessage>> WriteNoFlush(
        const StoredMessage& flatMessage
    );

    task<> FlushWorker();

public:
    SequentialMessageWriter(
        shared_ptr<RandomMessageWriter> randomMessageWriter);

    virtual task<DataReference<StoredMessage>> Write(
        const StoredMessage& flatMessage,
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
    std::unordered_map<
        FlatValue<ExtentName>,
        shared_ptr<RandomMessageReader>,
        ProtoValueStlHash,
        ProtoValueStlEqual
    > m_readableExtents;

    task<shared_ptr<RandomMessageReader>> OpenExtentForRandomReadAccessImpl(
        shared_ptr<IReadableExtent> readableExtent);

    task<shared_ptr<RandomMessageReader>> OpenExtentForRandomReadAccessImpl(
        const ExtentName* ExtentName);

    task<shared_ptr<RandomMessageWriter>> OpenExtentForRandomWriteAccessImpl(
        const ExtentName* ExtentName);

public:
    MessageStore(
        Schedulers schedulers,
        shared_ptr<IExtentStore> extentStore);

    // Inherited via IMessageStore
    virtual task<shared_ptr<IRandomMessageReader>> OpenExtentForRandomReadAccess(
        const shared_ptr<IReadableExtent>& readableExtent
    ) override;

    virtual task<shared_ptr<IRandomMessageReader>> OpenExtentForRandomReadAccess(
        const FlatBuffers::ExtentName* extentName
    ) override;

    virtual task<shared_ptr<IRandomMessageWriter>> OpenExtentForRandomWriteAccess(
        const FlatBuffers::ExtentName* extentName
    ) override;

    virtual task<shared_ptr<ISequentialMessageReader>> OpenExtentForSequentialReadAccess(
        const shared_ptr<IReadableExtent>& readableExtent
    ) override;

    virtual task<shared_ptr<ISequentialMessageReader>> OpenExtentForSequentialReadAccess(
        const FlatBuffers::ExtentName* extentName
    ) override;

    virtual task<shared_ptr<ISequentialMessageWriter>> OpenExtentForSequentialWriteAccess(
        const FlatBuffers::ExtentName* extentName
    ) override;
};
}
