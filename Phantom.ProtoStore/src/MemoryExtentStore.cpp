#include "ExtentName.h"
#include "MemoryExtentStore.h"
#include "Resources.h"
#include "StandardTypes.h"
#include <assert.h>
#include <cppcoro/async_mutex.hpp>
#include <functional>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <list>

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
        std::shared_ptr<vector<uint8_t>> m_bytes;
        std::list<WriteOperation> m_pendingWriteOperations;
        Schedulers m_schedulers;

        // This is here for debugging purposes.
        FlatValue<ExtentName> m_extentName;

        class WriteBuffer
            :
            public IWriteBuffer
        {
            Extent* m_extent;
            std::shared_ptr<WriteOperation> m_currentWriteOperation;

            WritableRawData StartWriteOperation(
                size_t offset,
                size_t count)
            {
                assert(!m_currentWriteOperation);

                m_currentWriteOperation = std::make_shared<WriteOperation>(WriteOperation
                    {
                        offset,
                        vector<uint8_t>(count),
                    });

                return WritableRawData
                {
                    m_currentWriteOperation,
                    {
                        reinterpret_cast<byte*>(m_currentWriteOperation->m_bytes.data()),
                        m_currentWriteOperation->m_bytes.size()
                    }
                };
            }

        public:
            WriteBuffer(
                Extent* extent)
                :
                m_extent(extent)
            {
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
                        *m_currentWriteOperation);
                    m_currentWriteOperation.reset();
                }
                co_return;
            }

            virtual task<WritableRawData> Write(
                ExtentOffset offset,
                size_t count)
            {
                co_await Commit();

                co_return StartWriteOperation(
                    offset,
                    count);
            }
        };

        task<RawData> Read(
            ExtentOffset offset,
            size_t count
        )
        {
            auto lock = co_await m_mutex.scoped_lock_async();
                
            if (offset > m_bytes->size())
            {
                co_return{};
            }
            if (offset + count > m_bytes->size())
            {
                co_return{};
            }

            co_return RawData(
                m_bytes,
                {
                    reinterpret_cast<const byte*>(m_bytes->data() + offset),
                    count
                }
            );
        }

        virtual task<pooled_ptr<IWriteBuffer>> CreateWriteBuffer() override
        {
            pooled_ptr<IWriteBuffer> writeBuffer(new WriteBuffer(
                this));

            co_return move(
                writeBuffer);
        }

        task<> Write(
            WriteOperation writeOperation
        )
        {
            auto lock = co_await m_mutex.scoped_lock_async();

            co_await m_schedulers.LockScheduler->schedule();

            m_pendingWriteOperations.push_back(
                move(
                    writeOperation));
        }

        virtual task<> Flush() override
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
            FlatValue<ExtentName> extentName,
            Schedulers schedulers)
            :
            m_extentName(extentName),
            m_schedulers(schedulers),
            m_bytes(make_shared<vector<uint8_t>>())
        {}
    };

    unordered_map<
        FlatValue<ExtentName>, 
        shared_ptr<Extent>,
        ProtoValueStlHash,
        ProtoValueStlEqual
    > m_extents;
    cppcoro::async_mutex m_extentsMutex;

    task<shared_ptr<Extent>> GetExtent(
        const ExtentName* extentName,
        bool replace)
    {
        auto lock = co_await m_extentsMutex.scoped_lock_async();

        if (replace)
        {
            m_extents.erase(
                FlatValue{ extentName });
        }
            
        auto existingExtent = m_extents.find(
            FlatValue{ extentName });

        if (existingExtent != m_extents.end())
        {
            co_return existingExtent->second;
        }

        auto newExtent = make_shared<Extent>(
            Clone(extentName),
            m_schedulers);
        
        m_extents[Clone(extentName)] = newExtent;
        co_return newExtent;
    }

    Schedulers m_schedulers;

public:
    Impl(
        Schedulers schedulers
    ) : 
        m_schedulers(schedulers),
        m_extents(
            0,
            FlatBuffersSchemas_Comparers().ExtentName_Comparers.hash,
            FlatBuffersSchemas_Comparers().ExtentName_Comparers.equal_to
        )
    {}

    Impl(const Impl& other)
        :
        m_extents(other.m_extents)
    {}

    task<shared_ptr<IReadableExtent>> OpenExtentForRead(
        const ExtentName* extentName)
    {
        co_return co_await GetExtent(
            extentName,
            false);
    }

    task<shared_ptr<IWritableExtent>> OpenExtentForWrite(
        const ExtentName* extentName)
    {

        co_return co_await GetExtent(
            extentName,
            true);
    }

    task<> DeleteExtent(
        const ExtentName* extentName)
    {
        auto lock = co_await m_extentsMutex.scoped_lock_async();

        m_extents.erase(
            FlatValue{ extentName });
    }

    task<bool> ExtentExists(
        const ExtentName* extentName)
    {
        auto lock = co_await m_extentsMutex.scoped_lock_async();

        co_return m_extents.contains(
            FlatValue{ extentName });
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
    const ExtentName* extentName)
{
    return m_impl->OpenExtentForRead(
        extentName);
}

task<shared_ptr<IWritableExtent>> MemoryExtentStore::OpenExtentForWrite(
    const ExtentName* extentName)
{
    return m_impl->OpenExtentForWrite(
        extentName);
}

task<> MemoryExtentStore::DeleteExtent(
    const ExtentName* extentName)
{
    return m_impl->DeleteExtent(
        extentName);
}

task<bool> MemoryExtentStore::ExtentExists(
    const ExtentName* extentName)
{
    return m_impl->ExtentExists(
        extentName);
}

}
