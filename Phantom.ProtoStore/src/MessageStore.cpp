#include "Checksum.h"
#include "MessageStore.h"
#include "ExtentStore.h"
#include <cppcoro/async_mutex.hpp>

namespace Phantom::ProtoStore
{
    using google::protobuf::io::CodedInputStream;
    using google::protobuf::io::CodedOutputStream;

    class RandomMessageReader 
        : 
        public IRandomMessageReader
    {
        shared_ptr<IReadableExtent> m_extent;
        shared_ptr<IChecksumAlgorithmFactory> m_checksumAlgorithmFactory;

    public:
        RandomMessageReader(
            shared_ptr<IReadableExtent> extent,
            shared_ptr<IChecksumAlgorithmFactory> checksumAlgorithmFactory)
            :
            m_extent(extent),
            m_checksumAlgorithmFactory(checksumAlgorithmFactory)
        {}

        virtual task<ReadMessageResult> Read(
            ExtentOffset extentOffset,
            Message& message
        ) override
        {
            auto messageHeaderReadBufferSize =
                // The message size.
                sizeof(uint32_t)
                +
                // The checksum algorithm.
                sizeof(ChecksumAlgorithmVersion);

            auto messageHeaderReadBuffer = co_await m_extent->Read(
                extentOffset,
                messageHeaderReadBufferSize);

            CodedInputStream messageHeaderInputStream(
                messageHeaderReadBuffer->Stream());

            google::protobuf::uint32 messageSize;

            if (!messageHeaderInputStream.ReadLittleEndian32(
                &messageSize))
            {
                throw std::range_error("Invalid message size.");
            }

            ChecksumAlgorithmVersion checksumVersion;

            if (!messageHeaderInputStream.ReadRaw(
                &checksumVersion,
                sizeof(checksumVersion)))
            {
                throw std::range_error("Invalid checksum version.");
            }

            auto checksum = m_checksumAlgorithmFactory->Create(
                checksumVersion);

            auto messageDataBuffer = co_await m_extent->Read(
                extentOffset + messageHeaderReadBufferSize,
                messageSize + checksum->SizeInBytes());

            {
                google::protobuf::io::LimitingInputStream messageDataLimitingStream(
                    messageDataBuffer->Stream(),
                    messageSize);

                ChecksummingZeroCopyInputStream checksummingInputStream(
                    &messageDataLimitingStream,
                    checksum.get());

                message.ParseFromZeroCopyStream(
                    &checksummingInputStream);
            }
            checksum->Finalize();

            {
                CodedInputStream checksumInputStream(
                    messageDataBuffer->Stream());

                if (!checksumInputStream.ReadRaw(
                    checksum->Comparand().data(),
                    checksum->Comparand().size_bytes()))
                {
                    throw std::range_error("Invalid checksum data.");
                }
            }

            if (!checksum->IsValid())
            {
                throw std::range_error("Checksum failed.");
            }

            co_return ReadMessageResult
            {
                extentOffset + messageHeaderReadBufferSize + messageSize,
            };
        }
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
            shared_ptr<IChecksumAlgorithmFactory> checksumAlgorithmFactory)
            :
            m_extent(extent),
            m_checksumAlgorithmFactory(checksumAlgorithmFactory)
        {}

        virtual task<WriteMessageResult> Write(
            ExtentOffset extentOffset,
            const Message& message
        ) override
        {
            auto checksum = m_checksumAlgorithmFactory->Create();

            auto messageHeaderWriteBufferSize =
                sizeof(uint32_t)
                +
                sizeof(ChecksumAlgorithmVersion);

            auto messageHeaderWriteBuffer = co_await m_extent->Write(
                extentOffset,
                messageHeaderWriteBufferSize);

            CodedOutputStream messageHeaderOutputStream(
                messageHeaderWriteBuffer->Stream());

            auto messageSize = message.ByteSizeLong();
            auto messageSize32 = static_cast<uint32_t>(messageSize);
            auto messageDataSize = messageSize + checksum->SizeInBytes();
            auto messageDataSize32 = static_cast<uint32_t>(messageDataSize);

            if (messageDataSize != messageDataSize32)
            {
                throw std::range_error("Message too big.");
            }

            auto checksumVersion = checksum->Version();

            messageHeaderOutputStream.WriteLittleEndian32(
                messageSize);
            messageHeaderOutputStream.WriteRaw(
                &checksumVersion,
                sizeof(checksumVersion));

            auto messageDataBuffer = co_await m_extent->Write(
                extentOffset + messageHeaderWriteBufferSize,
                messageDataSize32);

            {
                ChecksummingZeroCopyOutputStream checksummingOutputStream(
                    messageDataBuffer->Stream(),
                    checksum.get());

                message.SerializeToZeroCopyStream(
                    &checksummingOutputStream);
            }
            checksum->Finalize();

            if (!checksum->IsValid())
            {
                throw std::range_error("Checksum failed.");
            }

            co_return WriteMessageResult
            {
                extentOffset + messageHeaderWriteBufferSize + messageDataSize,
            };
        }
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
            ExtentNumber extentNumber)
        {
            auto lock = co_await m_asyncMutex.scoped_lock_async();

            auto readableExtent = m_readableExtents[extentNumber].lock();
            if (!readableExtent)
            {
                readableExtent = co_await m_extentStore->OpenExtentForRead(
                    extentNumber);
                m_readableExtents[extentNumber] = readableExtent;
            }

            co_return readableExtent;
        }

        task<shared_ptr<IWritableExtent>> OpenExtentForWrite(
            ExtentNumber extentNumber)
        {
            auto lock = co_await m_asyncMutex.scoped_lock_async();

            auto& writableExtent = m_writableExtents[extentNumber].lock();
            if (!writableExtent)
            {
                writableExtent = co_await m_extentStore->OpenExtentForWrite(
                    extentNumber);
                m_writableExtents[extentNumber] = writableExtent;
            }

            co_return writableExtent;
        }

    public:
        MessageStore(
            shared_ptr<IExtentStore> extentStore)
            :
            m_extentStore(extentStore),
            m_checksumAlgorithmFactory(MakeChecksumAlgorithmFactory())
        {
        }

        // Inherited via IMessageStore
        virtual task<shared_ptr<IRandomMessageReader>> OpenExtentForRandomReadAccess(
            ExtentNumber extentNumber
        ) override
        {
            auto readableExtent = co_await OpenExtentForRead(
                extentNumber);

            co_return make_shared<RandomMessageReader>(
                readableExtent,
                m_checksumAlgorithmFactory);
        }

        virtual task<shared_ptr<IRandomMessageWriter>> OpenExtentForRandomWriteAccess(
            ExtentNumber extentNumber
        ) override
        {
            auto writableExtent = co_await OpenExtentForWrite(
                extentNumber);

            co_return make_shared<RandomMessageWriter>(
                writableExtent,
                m_checksumAlgorithmFactory);
        }

        virtual task<shared_ptr<ISequentialMessageWriter>> OpenExtentForSequentialReadAccess(
            ExtentNumber extentNumber
        ) override
        {
            return task<shared_ptr<ISequentialMessageWriter>>();
        }

        virtual task<shared_ptr<ISequentialMessageWriter>> OpenExtentForSequentialWriteAccess(
            ExtentNumber extentNumber
        ) override
        {
            return task<shared_ptr<ISequentialMessageWriter>>();
        }
    };

    task<shared_ptr<IMessageStore>> CreateMessageStore(
        shared_ptr<IExtentStore> extentStore)
    {
        co_return std::make_shared<MessageStore>(
            extentStore);
    }
}
