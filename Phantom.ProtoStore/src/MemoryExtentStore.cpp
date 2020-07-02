#include "MemoryExtentStore.h"
#include <assert.h>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <cppcoro/async_mutex.hpp>

namespace Phantom::ProtoStore
{
    using std::shared_ptr;
    using std::optional;

    class MemoryExtentStore::Impl
    {
        class Extent 
            : 
            public IReadableExtent,
            public IWritableExtent
        {
            cppcoro::async_mutex m_mutex;
            std::shared_ptr<vector<uint8_t>> m_bytes;

            class ReadBuffer
                :
                public IReadBuffer
            {
                Extent* m_extent;
                std::shared_ptr<vector<uint8_t>> m_bytes;
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
                        throw std::range_error("Read past end");
                    }

                    m_inputStream.emplace(
                        m_bytes->data() + offset,
                        count);
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

            struct WriteOperation
            {
                ExtentOffset m_offset;
                std::vector<uint8_t> m_bytes;
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
                        std::vector<uint8_t>(count),
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

                    while (m_writeOperations.size() > 0)
                    {
                        co_await m_extent->Write(
                            m_writeOperations.front());
                        m_writeOperations.pop_front();
                    }
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
                        m_writeOperations.push_back(
                            *m_currentWriteOperation);
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

                co_return std::move(readBuffer);
            }

            task<shared_ptr<vector<uint8_t>>> Read(
                ExtentOffset offset,
                size_t count
            )
            {
                auto lock = co_await m_mutex.scoped_lock_async();
                co_return m_bytes;
            }

            virtual task<pooled_ptr<IWriteBuffer>> CreateWriteBuffer() override
            {
                pooled_ptr<IWriteBuffer> writeBuffer(new WriteBuffer(
                    this));

                co_return std::move(
                    writeBuffer);
            }

            task<> Write(
                WriteOperation& writeOperation
                )
            {
                auto lock = co_await m_mutex.scoped_lock_async();

                auto neededSize = writeOperation.m_bytes.size() + writeOperation.m_offset;

                if (neededSize > m_bytes->size())
                {
                    auto newBytes = make_shared<vector<uint8_t>>(
                        *m_bytes);

                    newBytes->resize(
                        neededSize);

                    m_bytes = newBytes;
                }

                std::copy(
                    writeOperation.m_bytes.begin(),
                    writeOperation.m_bytes.end(),
                    m_bytes->begin() + writeOperation.m_offset);
            }
        public:
            Extent()
                :
                m_bytes(std::make_shared<vector<uint8_t>>())
            {}
        };

        std::map<ExtentNumber, std::shared_ptr<Extent>> m_extents;
        std::mutex m_extentsMutex;

        shared_ptr<Extent> GetExtent(
            ExtentNumber extentNumber)
        {
            std::scoped_lock lock(m_extentsMutex);

            auto existingExtent = m_extents.find(
                extentNumber);

            if (existingExtent != m_extents.end())
            {
                return existingExtent->second;
            }

            auto newExtent = std::make_shared<Extent>();
            m_extents[extentNumber] = newExtent;
            return newExtent;
        }
        
    public:
        Impl()
        {}

        Impl(const Impl& other)
            :
            m_extents(other.m_extents)
        {}

        task<shared_ptr<IReadableExtent>> OpenExtentForRead(
            ExtentNumber extentNumber)
        {
            co_return GetExtent(
                extentNumber);
        }

        task<shared_ptr<IWritableExtent>> OpenExtentForWrite(
            ExtentNumber extentNumber)
        {
            co_return GetExtent(
                extentNumber);
        }

        task<> DeleteExtent(
            ExtentNumber extentNumber)
        {
            {
                std::scoped_lock lock(m_extentsMutex);
                m_extents.erase(
                    extentNumber);
            }

            co_return;
        }
    };

    MemoryExtentStore::MemoryExtentStore()
        : m_impl(new Impl())
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

}
