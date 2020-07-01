#include "MemoryExtentStore.h"
#include <functional>
#include <map>
#include <mutex>
#include <vector>
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
            cppcoro::async_mutex m_mutex;
            std::shared_ptr<vector<uint8_t>> m_bytes;

            class ReadBuffer
                :
                public IReadBuffer
            {
                std::shared_ptr<vector<uint8_t>> m_bytes;
                google::protobuf::io::ArrayInputStream m_inputStream;

            public:
                ReadBuffer(
                    std::shared_ptr<vector<uint8_t>> bytes,
                    ExtentOffset offset,
                    size_t count)
                    :
                    m_bytes(bytes),
                    m_inputStream(bytes->data() + offset, count)
                {
                    if (m_bytes->size() < offset + count)
                    {
                        throw std::out_of_range("Reading past end of stream");
                    }
                }

                virtual google::protobuf::io::ZeroCopyInputStream* Stream() override
                {
                    return &m_inputStream;
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
                std::shared_ptr<vector<uint8_t>> m_originalBytes;
                google::protobuf::io::ArrayOutputStream m_outputStream;
                ExtentOffset m_offset;
                size_t m_count;
            public:
                WriteBuffer(
                    Extent* extent,
                    std::shared_ptr<vector<uint8_t>> bytes,
                    ExtentOffset offset,
                    size_t count)
                    :
                    m_extent(extent),
                    m_originalBytes(bytes),
                    m_outputStream(bytes->data() + offset, count),
                    m_offset(offset),
                    m_count(count)
                {
                }

                virtual google::protobuf::io::ZeroCopyOutputStream* Stream() override
                {
                    return &m_outputStream;
                }

                virtual task<> Flush() override
                {
                    return m_extent->Write(
                        m_originalBytes,
                        m_offset,
                        m_count);
                }

                virtual void ReturnToPool()
                {
                    delete this;
                }
            };

            virtual task<pooled_ptr<IReadBuffer>> Read(
                ExtentOffset offset,
                size_t count
            ) override
            {
                auto lock = co_await m_mutex.scoped_lock_async();

                pooled_ptr<IReadBuffer> readBuffer(new ReadBuffer(
                    m_bytes,
                    offset,
                    count));

                co_return std::move(readBuffer);
            }

            virtual task<pooled_ptr<IWriteBuffer>> Write(
                ExtentOffset offset,
                size_t count
            ) override
            {
                auto lock = co_await m_mutex.scoped_lock_async();

                if (offset + count > m_bytes->capacity())
                {
                    auto newBytes = std::make_shared<std::vector<uint8_t>>();
                    newBytes->reserve(
                        m_bytes->capacity() * 2);
                    newBytes->resize(
                        offset + count);

                    std::copy(
                        m_bytes->begin(), 
                        m_bytes->end(), 
                        newBytes->begin());
                        
                    m_bytes = newBytes;
                }

                pooled_ptr<IWriteBuffer> writeBuffer(new WriteBuffer(
                    this,
                    m_bytes,
                    offset,
                    count));

                co_return std::move(
                    writeBuffer);
            }

            task<> Write(
                std::shared_ptr<vector<uint8_t>> m_originalBytes,
                ExtentOffset offset,
                size_t count
                )
            {
                auto lock = co_await m_mutex.scoped_lock_async();

                if (m_originalBytes == m_bytes)
                {
                    return;
                }

                std::copy(
                    m_originalBytes->begin() + offset,
                    m_originalBytes->begin() + offset + count,
                    m_bytes->begin() + offset);
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
