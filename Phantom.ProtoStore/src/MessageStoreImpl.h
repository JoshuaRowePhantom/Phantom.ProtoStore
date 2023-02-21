#pragma once

#include "StandardTypes.h"
#include "ExtentName.h"
#include <cppcoro/async_mutex.hpp>
#include "MessageStore.h"
#include "Checksum.h"
#include "Phantom.System/async_reader_writer_lock.h"

namespace Phantom::ProtoStore
{
    class RandomMessageReader
        :
        public IRandomMessageReader
    {
        const shared_ptr<IReadableExtent> m_extent;
        const shared_ptr<IChecksumAlgorithmFactory> m_checksumAlgorithmFactory;

    public:
        RandomMessageReader(
            shared_ptr<IReadableExtent> extent,
            shared_ptr<IChecksumAlgorithmFactory> checksumAlgorithmFactory);

        virtual task<StoredFlatMessage> ReadFlatMessage(
            ExtentOffset extentOffset
        ) override;

        virtual task<ReadProtoMessageResult> Read(
            ExtentOffset extentOffset,
            Message& message
        ) override;
    };

    class RandomMessageWriter
        :
        public IRandomMessageWriter
    {
        const shared_ptr<IWritableExtent> m_extent;
        const shared_ptr<IChecksumAlgorithmFactory> m_checksumAlgorithmFactory;
        const size_t m_checksumSize;

    public:
        RandomMessageWriter(
            shared_ptr<IWritableExtent> extent,
            shared_ptr<IChecksumAlgorithmFactory> checksumAlgorithmFactory);

        WriteMessageResult GetWriteMessageResult(
            ExtentOffset extentOffset,
            size_t messageSize);

        task<> Write(
            const WriteMessageResult& writeMessageResult,
            const Message& message,
            FlushBehavior flushBehavior
        );

        virtual task<StoredFlatMessage> WriteFlatMessage(
            ExtentOffset extentOffset,
            StoredFlatMessage message,
            FlushBehavior flushBehavior
        ) override;

        virtual task<WriteMessageResult> Write(
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

        virtual task<StoredFlatMessage> ReadFlatMessage(
        ) override;

        virtual task<ReadProtoMessageResult> Read(
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

        virtual task<StoredFlatMessage> WriteFlatMessage(
            StoredFlatMessage flatMessage,
            FlushBehavior flushBehavior
        ) override;

        virtual task<WriteMessageResult> Write(
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
        std::unordered_map<ExtentName, shared_ptr<IReadableExtent>> m_readableExtents;
        shared_ptr<IChecksumAlgorithmFactory> m_checksumAlgorithmFactory;

        task<shared_ptr<IReadableExtent>> OpenExtentForRead(
            const ExtentName& ExtentName);

        task<shared_ptr<IWritableExtent>> OpenExtentForWrite(
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
