#include "StandardTypes.h"
#include "Checksum.h"
#include "MessageStoreImpl.h"
#include "ExtentStore.h"
#include <cppcoro/async_mutex.hpp>

namespace Phantom::ProtoStore
{
    using google::protobuf::io::CodedInputStream;
    using google::protobuf::io::CodedOutputStream;
    
    RandomMessageReader::RandomMessageReader(
        shared_ptr<IReadableExtent> extent,
        shared_ptr<IChecksumAlgorithmFactory> checksumAlgorithmFactory)
        :
        m_extent(extent),
        m_checksumAlgorithmFactory(checksumAlgorithmFactory)
    {}

    task<ReadMessageResult> RandomMessageReader::Read(
        ExtentOffset extentOffset,
        Message& message
    )
    {
        auto messageHeaderReadBufferSize =
            // The message size.
            sizeof(uint32_t)
            +
            // The checksum algorithm.
            sizeof(ChecksumAlgorithmVersion);

        auto readBuffer = co_await m_extent->CreateReadBuffer();
        co_await readBuffer->Read(
            extentOffset,
            messageHeaderReadBufferSize);

        CodedInputStream messageHeaderInputStream(
            readBuffer->Stream());

        google::protobuf::uint32 messageSize;

        if (!messageHeaderInputStream.ReadLittleEndian32(
            &messageSize))
        {
            throw range_error("Invalid message size.");
        }

        ChecksumAlgorithmVersion checksumVersion;

        if (!messageHeaderInputStream.ReadRaw(
            &checksumVersion,
            sizeof(checksumVersion)))
        {
            throw range_error("Invalid checksum version.");
        }

        auto checksum = m_checksumAlgorithmFactory->Create(
            checksumVersion);

        co_await readBuffer->Read(
            extentOffset + messageHeaderReadBufferSize,
            messageSize + checksum->SizeInBytes());

        {
            google::protobuf::io::LimitingInputStream messageDataLimitingStream(
                readBuffer->Stream(),
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
                readBuffer->Stream());

            if (!checksumInputStream.ReadRaw(
                checksum->Comparand().data(),
                static_cast<int>(checksum->Comparand().size_bytes())))
            {
                throw range_error("Invalid checksum data.");
            }
        }

        if (!checksum->IsValid())
        {
            throw range_error("Checksum failed.");
        }

        co_return ReadMessageResult
        {
            extentOffset 
            + messageHeaderReadBufferSize 
            + messageSize 
            + checksum->SizeInBytes(),
        };
    }

    RandomMessageWriter::RandomMessageWriter(
        shared_ptr<IWritableExtent> extent,
        shared_ptr<IChecksumAlgorithmFactory> checksumAlgorithmFactory)
        :
        m_extent(extent),
        m_checksumAlgorithmFactory(checksumAlgorithmFactory)
    {}

    task<WriteMessageResult> RandomMessageWriter::Write(
        ExtentOffset extentOffset,
        const Message& message,
        FlushBehavior flushBehavior
    )
    {
        auto checksum = m_checksumAlgorithmFactory->Create();

        auto messageHeaderWriteBufferSize =
            sizeof(uint32_t)
            +
            sizeof(ChecksumAlgorithmVersion);

        auto writeBuffer = co_await m_extent->CreateWriteBuffer();
        
        co_await writeBuffer->Write(
            extentOffset,
            messageHeaderWriteBufferSize);

        auto messageSize = message.ByteSizeLong();
        auto messageSize32 = static_cast<uint32_t>(messageSize);
        auto messageDataSize = messageSize + checksum->SizeInBytes();
        auto messageDataSize32 = static_cast<uint32_t>(messageDataSize);

        if (messageDataSize != messageDataSize32)
        {
            throw range_error("Message too big.");
        }

        auto checksumVersion = checksum->Version();

        {
            CodedOutputStream messageHeaderOutputStream(
                writeBuffer->Stream());

            messageHeaderOutputStream.WriteLittleEndian32(
                static_cast<google::protobuf::uint32>(messageSize));
            messageHeaderOutputStream.WriteRaw(
                &checksumVersion,
                sizeof(checksumVersion));
        }
        co_await writeBuffer->Commit();

        co_await writeBuffer->Write(
            extentOffset + messageHeaderWriteBufferSize,
            messageDataSize32);

        {
            ChecksummingZeroCopyOutputStream checksummingOutputStream(
                writeBuffer->Stream(),
                checksum.get());

            message.SerializeToZeroCopyStream(
                &checksummingOutputStream);
        }

        checksum->Finalize();

        {
            CodedOutputStream checksumOutputStream(
                writeBuffer->Stream());

            checksumOutputStream.WriteRaw(
                checksum->Computed().data(),
                static_cast<int>(checksum->Computed().size_bytes()));
        }

        if (flushBehavior == FlushBehavior::Flush)
        {
            co_await writeBuffer->Flush();
        }
        else
        {
            co_await writeBuffer->Commit();
        }

        co_return WriteMessageResult
        {
            extentOffset + messageHeaderWriteBufferSize + messageDataSize,
        };
    }

    task<shared_ptr<IReadableExtent>> MessageStore::OpenExtentForRead(
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

    task<shared_ptr<IWritableExtent>> MessageStore::OpenExtentForWrite(
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

    MessageStore::MessageStore(
        shared_ptr<IExtentStore> extentStore)
        :
        m_extentStore(move(extentStore)),
        m_checksumAlgorithmFactory(MakeChecksumAlgorithmFactory())
    {
    }

    // Inherited via IMessageStore
    task<shared_ptr<IRandomMessageReader>> MessageStore::OpenExtentForRandomReadAccess(
        ExtentNumber extentNumber
    )
    {
        auto readableExtent = co_await OpenExtentForRead(
            extentNumber);

        co_return make_shared<RandomMessageReader>(
            readableExtent,
            m_checksumAlgorithmFactory);
    }

    task<shared_ptr<IRandomMessageWriter>> MessageStore::OpenExtentForRandomWriteAccess(
        ExtentNumber extentNumber
    )
    {
        auto writableExtent = co_await OpenExtentForWrite(
            extentNumber);

        co_return make_shared<RandomMessageWriter>(
            writableExtent,
            m_checksumAlgorithmFactory);
    }

    task<shared_ptr<ISequentialMessageWriter>> MessageStore::OpenExtentForSequentialReadAccess(
        ExtentNumber extentNumber
    )
    {
        return task<shared_ptr<ISequentialMessageWriter>>();
    }

    task<shared_ptr<ISequentialMessageWriter>> MessageStore::OpenExtentForSequentialWriteAccess(
        ExtentNumber extentNumber
    ) 
    {
        return task<shared_ptr<ISequentialMessageWriter>>();
    }

    shared_ptr<IMessageStore> MakeMessageStore(
        shared_ptr<IExtentStore> extentStore)
    {
        return make_shared<MessageStore>(
            move(extentStore));
    }
}
