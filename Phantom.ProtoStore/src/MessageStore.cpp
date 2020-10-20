#include "StandardTypes.h"
#include "Checksum.h"
#include "MessageStoreImpl.h"
#include "ExtentStore.h"
#include <cppcoro/async_mutex.hpp>
#include "ExtentName.h"

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
            .DataRange =
            {
                .Beginning = extentOffset,
                .End =
                    extentOffset
                    + messageHeaderReadBufferSize
                    + messageSize
                    + checksum->SizeInBytes(),
            },
        };
    }

    RandomMessageWriter::RandomMessageWriter(
        shared_ptr<IWritableExtent> extent,
        shared_ptr<IChecksumAlgorithmFactory> checksumAlgorithmFactory)
        :
        m_extent(extent),
        m_checksumAlgorithmFactory(checksumAlgorithmFactory),
        m_checksumSize(checksumAlgorithmFactory->Create()->SizeInBytes())
    {
    }

    WriteMessageResult RandomMessageWriter::GetWriteMessageResult(
        ExtentOffset extentOffset,
        size_t messageSize
    )
    {
        WriteMessageResult result;
        result.MessageLengthRange =
        {
            .Beginning = extentOffset,
            .End = extentOffset + sizeof(google::protobuf::uint32),
        };
        result.ChecksumAlgorithmRange =
        {
            .Beginning = result.MessageLengthRange.End,
            .End = result.MessageLengthRange.End + sizeof(ChecksumAlgorithmVersion),
        };
        result.MessageRange =
        {
            .Beginning = result.ChecksumAlgorithmRange.End,
            .End = result.ChecksumAlgorithmRange.End + messageSize,
        };
        result.ChecksumRange =
        {
            .Beginning = result.MessageRange.End,
            .End = result.MessageRange.End + m_checksumSize,
        };
        result.DataRange =
        {
            .Beginning = extentOffset,
            .End = result.ChecksumRange.End,
        };

        auto dataSize = result.DataRange.End - result.DataRange.Beginning;
        auto dataSize32 = static_cast<uint32_t>(dataSize);
        if (dataSize != dataSize32)
        {
            throw range_error("Message too big.");
        }

        return result;
    }

    task<WriteMessageResult> RandomMessageWriter::Write(
        ExtentOffset extentOffset,
        const Message& message,
        FlushBehavior flushBehavior
    )
    {
        auto writeMessageResult = GetWriteMessageResult(
            extentOffset,
            message.ByteSizeLong());

        co_await Write(
            writeMessageResult,
            message,
            flushBehavior);

        co_return writeMessageResult;
    }

    task<> RandomMessageWriter::Write(
        const WriteMessageResult& writeMessageResult,
        const Message& message,
        FlushBehavior flushBehavior
    )
    {
        auto checksum = m_checksumAlgorithmFactory->Create();

        auto writeBufferSize = writeMessageResult.DataRange.End - writeMessageResult.DataRange.Beginning;
        auto messageSize = writeMessageResult.MessageRange.End - writeMessageResult.MessageRange.Beginning;
        auto writeBuffer = co_await m_extent->CreateWriteBuffer();

        co_await writeBuffer->Write(
            writeMessageResult.DataRange.Beginning,
            writeBufferSize);

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
    }

    SequentialMessageReader::SequentialMessageReader(
        shared_ptr<RandomMessageReader> randomMessageReader
    )
        : m_randomMessageReader(randomMessageReader),
        m_currentOffset(0)
    {
    }

    task<ReadMessageResult> SequentialMessageReader::Read(
        Message& message
    )
    {
        auto readResult = co_await m_randomMessageReader->Read(
            m_currentOffset,
            message);

        m_currentOffset = readResult.DataRange.End;

        co_return readResult;
    }

    SequentialMessageWriter::SequentialMessageWriter(
        shared_ptr<RandomMessageWriter> randomMessageWriter
    )
        : 
        m_randomMessageWriter(randomMessageWriter),
        m_currentOffset(0)
    {}

    task<WriteMessageResult> SequentialMessageWriter::Write(
        const Message& message,
        FlushBehavior flushBehavior
    )
    {
        auto messageSize = message.ByteSizeLong();
        WriteMessageResult writeMessageResult;

        while (true)
        {
            auto offset = m_currentOffset.load(
                std::memory_order_relaxed);

            writeMessageResult = m_randomMessageWriter->GetWriteMessageResult(
                offset,
                messageSize);

            if (m_currentOffset.compare_exchange_strong(
                offset,
                writeMessageResult.DataRange.End,
                std::memory_order_acq_rel))
            {
                break;
            }
        }

        co_await m_randomMessageWriter->Write(
            writeMessageResult,
            message,
            flushBehavior);

        co_return writeMessageResult;
    }

    task<ExtentOffset> SequentialMessageWriter::CurrentOffset(
    )
    {
        co_return m_currentOffset.load(
            std::memory_order_acquire);
    }

    task<shared_ptr<IReadableExtent>> MessageStore::OpenExtentForRead(
        ExtentName extentName)
    {
        shared_ptr<IReadableExtent> readableExtent;

        co_await execute_conditional_read_unlikely_write_operation(
            m_extentsLock,
            *m_schedulers.LockScheduler,
            [&](auto hasWriteLock) -> task<bool>
        {
            readableExtent = m_readableExtents[extentName];
            co_return readableExtent != nullptr;
        },
            [&]() -> task<>
        {
            m_readableExtents[extentName]
                = readableExtent 
                = co_await m_extentStore->OpenExtentForRead(
                    extentName);
        });

        co_return readableExtent;
    }

    task<shared_ptr<IWritableExtent>> MessageStore::OpenExtentForWrite(
        ExtentName extentName)
    {
        shared_ptr<IWritableExtent> writableExtent;

        co_await execute_conditional_read_unlikely_write_operation(
            m_extentsLock,
            *m_schedulers.LockScheduler,
            [&](auto hasWriteLock) -> task<bool>
        {
            writableExtent = m_writableExtents[extentName];
            co_return writableExtent != nullptr;
        },
            [&]() -> task<>
        {
            m_writableExtents[extentName]
                = writableExtent 
                = co_await m_extentStore->OpenExtentForWrite(
                    extentName);
        });

        co_return writableExtent;
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
        co_return make_shared<RandomMessageReader>(
            readableExtent,
            m_checksumAlgorithmFactory);
    }

    task<shared_ptr<IRandomMessageReader>> MessageStore::OpenExtentForRandomReadAccess(
        ExtentName extentName
    )
    {
        auto readableExtent = co_await OpenExtentForRead(
            extentName);

        co_return make_shared<RandomMessageReader>(
            readableExtent,
            m_checksumAlgorithmFactory);
    }

    task<shared_ptr<IRandomMessageWriter>> MessageStore::OpenExtentForRandomWriteAccess(
        ExtentName extentName
    )
    {
        auto writableExtent = co_await OpenExtentForWrite(
            extentName);

        co_return make_shared<RandomMessageWriter>(
            writableExtent,
            m_checksumAlgorithmFactory);
    }

    task<shared_ptr<ISequentialMessageReader>> MessageStore::OpenExtentForSequentialReadAccess(
        const shared_ptr<IReadableExtent>& readableExtent
    )
    {
        auto randomMessageReader = make_shared<RandomMessageReader>(
            readableExtent,
            m_checksumAlgorithmFactory);

        co_return make_shared<SequentialMessageReader>(
            randomMessageReader);
    }

    task<shared_ptr<ISequentialMessageReader>> MessageStore::OpenExtentForSequentialReadAccess(
        ExtentName extentName
    )
    {
        auto readableExtent = co_await OpenExtentForRead(
            extentName);

        co_return co_await OpenExtentForSequentialReadAccess(
            readableExtent);
        
    }

    task<shared_ptr<ISequentialMessageWriter>> MessageStore::OpenExtentForSequentialWriteAccess(
        ExtentName extentName
    ) 
    {
        auto writableExtent = co_await OpenExtentForWrite(
            extentName);

        auto randomMessageWriter = make_shared<RandomMessageWriter>(
            writableExtent,
            m_checksumAlgorithmFactory);

        co_return make_shared<SequentialMessageWriter>(
            randomMessageWriter);
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
