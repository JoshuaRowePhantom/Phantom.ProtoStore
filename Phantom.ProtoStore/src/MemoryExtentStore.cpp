#include "StandardTypes.h"
#include "MemoryExtentStore.h"
#include <assert.h>
#include <functional>
#include <list>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <cppcoro/async_mutex.hpp>

namespace Phantom::ProtoStore
{
    class MemoryExtentStore::Impl
    {
        class Extent 
            : 
            public IReadableExtent,
            public IWritableExtent
        {
            struct WriteOperation
            {
                ExtentOffset m_offset;
                vector<uint8_t> m_bytes;
            };

            cppcoro::async_mutex m_mutex;
            shared_ptr<vector<uint8_t>> m_bytes;
            std::list<WriteOperation> m_pendingWriteOperations;
            Schedulers m_schedulers;

            class ReadBuffer
                :
                public IReadBuffer
            {
                Extent* m_extent;
                shared_ptr<vector<uint8_t>> m_bytes;
                optional<google::protobuf::io::ArrayInputStream> m_inputStream;

            public:
                ReadBuffer(
                    Extent* extent)
                    :
                    m_extent(extent)
                {
                }

                virtual task<> Read(
                    ExtentOffset offset,
                    size_t count
                ) override
                {
                    m_inputStream.reset();

                    m_bytes = co_await m_extent->Read(
                        offset,
                        count);
                    
                    if (m_bytes->size() < offset + count)
                    {
                        throw range_error("Read past end");
                    }

                    m_inputStream.emplace(
                        m_bytes->data() + offset,
                        static_cast<int>(count));
                }

                virtual google::protobuf::io::ZeroCopyInputStream* Stream() override
                {
                    return &*m_inputStream;
                }

                virtual void ReturnToPool()
                {
                    delete this;
                }
            };

            class WriteBuffer
                :
                public IWriteBuffer
            {
                Extent* m_extent;
                std::list<WriteOperation> m_writeOperations;
                optional<WriteOperation> m_currentWriteOperation;
                optional<google::protobuf::io::ArrayOutputStream> m_outputStream;

                void StartWriteOperation(
                    size_t offset,
                    size_t count)
                {
                    assert(!m_currentWriteOperation);

                    m_currentWriteOperation = WriteOperation
                    {
                        offset,
                        vector<uint8_t>(count),
                    };

                    m_outputStream.emplace(
                        m_currentWriteOperation->m_bytes.data(),
                        static_cast<int>(m_currentWriteOperation->m_bytes.size()),
                        3);
                }

            public:
                WriteBuffer(
                    Extent* extent)
                    :
                    m_extent(extent)
                {
                }

                virtual google::protobuf::io::ZeroCopyOutputStream* Stream() override
                {
                    assert(m_currentWriteOperation);
                    return &*m_outputStream;
                }

                virtual task<> Flush() override
                {
                    co_await Commit();
                    co_await m_extent->Flush();
                }

                virtual void ReturnToPool()
                {
                    assert(!m_currentWriteOperation);
                    delete this;
                }

                virtual task<> Commit() override
                {
                    if (m_currentWriteOperation)
                    {
                        co_await m_extent->Write(
                            move(*m_currentWriteOperation));
                        m_currentWriteOperation.reset();
                        m_outputStream.reset();
                    }
                    co_return;
                }

                virtual task<> Write(
                    ExtentOffset offset,
                    size_t count)
                {
                    co_await Commit();

                    StartWriteOperation(
                        offset,
                        count);
                }
            };

            virtual task<pooled_ptr<IReadBuffer>> CreateReadBuffer() override
            {
                pooled_ptr<IReadBuffer> readBuffer(new ReadBuffer(
                    this));

                co_return move(readBuffer);
            }

            task<shared_ptr<vector<uint8_t>>> Read(
                ExtentOffset offset,
                size_t count
            )
            {
                auto lock = co_await m_mutex.scoped_lock_async();

                co_await m_schedulers.LockScheduler->schedule();

                co_return m_bytes;
            }

            virtual task<pooled_ptr<IWriteBuffer>> CreateWriteBuffer() override
            {
                pooled_ptr<IWriteBuffer> writeBuffer(new WriteBuffer(
                    this));

                co_return move(
                    writeBuffer);
            }

            task<> Write(
                WriteOperation&& writeOperation
            )
            {
                auto lock = co_await m_mutex.scoped_lock_async();

                co_await m_schedulers.LockScheduler->schedule();

                m_pendingWriteOperations.push_back(
                    move(
                        writeOperation));
            }

            task<> Flush()
            {
                auto lock = co_await m_mutex.scoped_lock_async();

                co_await m_schedulers.LockScheduler->schedule();

                while (!m_pendingWriteOperations.empty())
                {
                    auto& writeOperation = m_pendingWriteOperations.front();

                    auto neededSize = writeOperation.m_bytes.size() + writeOperation.m_offset;

                    if (neededSize > m_bytes->size())
                    {
                        auto newBytes = make_shared<vector<uint8_t>>(
                            *m_bytes);

                        auto newSize = std::max(
                            newBytes->size() * 2,
                            neededSize);

                        newBytes->resize(
                            newSize);

                        m_bytes = newBytes;
                    }

                    std::copy(
                        writeOperation.m_bytes.begin(),
                        writeOperation.m_bytes.end(),
                        m_bytes->begin() + writeOperation.m_offset);

                    m_pendingWriteOperations.pop_front();
                }

            }

        public:
            Extent(
                Schedulers schedulers)
                :
                m_schedulers(schedulers),
                m_bytes(make_shared<vector<uint8_t>>())
            {}
        };

        map<ExtentNumber, shared_ptr<Extent>> m_extents;
        cppcoro::async_mutex m_extentsMutex;

        task<shared_ptr<Extent>> GetExtent(
            ExtentNumber extentNumber)
        {
            auto lock = co_await m_extentsMutex.scoped_lock_async();

            auto existingExtent = m_extents.find(
                extentNumber);

            if (existingExtent != m_extents.end())
            {
                co_return existingExtent->second;
            }

            auto newExtent = make_shared<Extent>(
                m_schedulers);
            m_extents[extentNumber] = newExtent;
            co_return newExtent;
        }
        
        Schedulers m_schedulers;

    public:
        Impl(
            Schedulers schedulers
        )
            : m_schedulers(schedulers)
        {}

        Impl(const Impl& other)
            :
            m_extents(other.m_extents)
        {}

        task<shared_ptr<IReadableExtent>> OpenExtentForRead(
            ExtentNumber extentNumber)
        {
            co_return co_await GetExtent(
                extentNumber);
        }

        task<shared_ptr<IWritableExtent>> OpenExtentForWrite(
            ExtentNumber extentNumber)
        {
            co_return co_await GetExtent(
                extentNumber);
        }

        task<> DeleteExtent(
            ExtentNumber extentNumber)
        {
            auto lock = co_await m_extentsMutex.scoped_lock_async();

            m_extents.erase(
                extentNumber);
        }

        task<bool> ExtentExists(
            ExtentNumber extentNumber)
        {
            auto lock = co_await m_extentsMutex.scoped_lock_async();

            co_return m_extents.contains(
                extentNumber);
        }
    };

    MemoryExtentStore::MemoryExtentStore(
        Schedulers schedulers)
        : m_impl(new Impl(
            schedulers))
    {}

    MemoryExtentStore::MemoryExtentStore(
        const MemoryExtentStore& other)
        : m_impl(new Impl(
            *other.m_impl))
    {}

    MemoryExtentStore::~MemoryExtentStore()
    {}

    task<shared_ptr<IReadableExtent>> MemoryExtentStore::OpenExtentForRead(
        ExtentNumber extentNumber)
    {
        return m_impl->OpenExtentForRead(
            extentNumber);
    }

    task<shared_ptr<IWritableExtent>> MemoryExtentStore::OpenExtentForWrite(
        ExtentNumber extentNumber)
    {
        return m_impl->OpenExtentForWrite(
            extentNumber);
    }

    task<> MemoryExtentStore::DeleteExtent(
        ExtentNumber extentNumber)
    {
        return m_impl->DeleteExtent(
            extentNumber);
    }

    task<bool> MemoryExtentStore::ExtentExists(
        ExtentNumber extentNumber)
    {
        return m_impl->ExtentExists(
            extentNumber);
    }

}
