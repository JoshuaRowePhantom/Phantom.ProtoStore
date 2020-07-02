#pragma once

#include <cppcoro/async_mutex.hpp>
#include "MessageStore.h"
#include "Checksum.h"

namespace Phantom::ProtoStore
{
    class RandomMessageReader
        :
        public IRandomMessageReader
    {
        shared_ptr<IReadableExtent> m_extent;
        shared_ptr<IChecksumAlgorithmFactory> m_checksumAlgorithmFactory;

    public:
        RandomMessageReader(
            shared_ptr<IReadableExtent> extent,
            shared_ptr<IChecksumAlgorithmFactory> checksumAlgorithmFactory);

        virtual task<ReadMessageResult> Read(
            ExtentOffset extentOffset,
            Message& message
        ) override;
    };

    class RandomMessageWriter
        :
        public IRandomMessageWriter
    {
        shared_ptr<IWritableExtent> m_extent;
        shared_ptr<IChecksumAlgorithmFactory> m_checksumAlgorithmFactory;

    public:
        RandomMessageWriter(
            shared_ptr<IWritableExtent> extent,
            shared_ptr<IChecksumAlgorithmFactory> checksumAlgorithmFactory);

        virtual task<WriteMessageResult> Write(
            ExtentOffset extentOffset,
            const Message& message
        ) override;
    };

    class MessageStore
        :
        public IMessageStore
    {
        shared_ptr<IExtentStore> m_extentStore;
        cppcoro::async_mutex m_asyncMutex;
        std::map<ExtentNumber, std::weak_ptr<IReadableExtent>> m_readableExtents;
        std::map<ExtentNumber, std::weak_ptr<IWritableExtent>> m_writableExtents;
        shared_ptr<IChecksumAlgorithmFactory> m_checksumAlgorithmFactory;

        task<shared_ptr<IReadableExtent>> OpenExtentForRead(
            ExtentNumber extentNumber);

        task<shared_ptr<IWritableExtent>> OpenExtentForWrite(
            ExtentNumber extentNumber);

    public:
        MessageStore(
            shared_ptr<IExtentStore> extentStore);

        // Inherited via IMessageStore
        virtual task<shared_ptr<IRandomMessageReader>> OpenExtentForRandomReadAccess(
            ExtentNumber extentNumber
        ) override;

        virtual task<shared_ptr<IRandomMessageWriter>> OpenExtentForRandomWriteAccess(
            ExtentNumber extentNumber
        ) override;

        virtual task<shared_ptr<ISequentialMessageWriter>> OpenExtentForSequentialReadAccess(
            ExtentNumber extentNumber
        ) override;

        virtual task<shared_ptr<ISequentialMessageWriter>> OpenExtentForSequentialWriteAccess(
            ExtentNumber extentNumber
        ) override;
    };

}
