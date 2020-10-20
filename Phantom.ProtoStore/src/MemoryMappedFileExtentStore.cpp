#include "MemoryMappedFileExtentStore.h"
#include <iomanip>
#include <map>
#include <fstream>
#include <sstream>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <filesystem>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <cppcoro/async_mutex.hpp>
#include "Phantom.System/async_reader_writer_lock.h"

using boost::interprocess::file_mapping;
using boost::interprocess::mapped_region;

namespace Phantom::ProtoStore
{

class MemoryMappedReadableExtent
    :
    public IReadableExtent
{
    mapped_region m_mappedRegion;
    
    friend class MemoryMappedReadBuffer;

public:
    MemoryMappedReadableExtent(
        mapped_region mappedRegion
    );

    virtual task<pooled_ptr<IReadBuffer>> CreateReadBuffer(
    ) override;
};

class MemoryMappedReadBuffer
    :
    public IReadBuffer
{
    MemoryMappedReadableExtent* m_extent;
    std::optional<google::protobuf::io::ArrayInputStream> m_inputStream;

public:
    MemoryMappedReadBuffer(
        MemoryMappedReadableExtent* memoryMappedReadableExtent);

    virtual task<> Read(
        ExtentOffset offset,
        size_t count
    ) override;

    virtual ZeroCopyInputStream* Stream(
    ) override;

    virtual void ReturnToPool(
    ) override;
};

namespace {
struct flush_region
{
    ExtentOffset beginning;
    ExtentOffset end;
};
}

class MemoryMappedWritableExtent;

class MemoryMappedWriteBuffer
    :
    public IWriteBuffer
{
    MemoryMappedWritableExtent* m_extent;
    shared_ptr<mapped_region> m_mappedRegion;
    flush_region m_flushRegion;
    std::optional<google::protobuf::io::ArrayOutputStream> m_stream;

public:
    MemoryMappedWriteBuffer(
        MemoryMappedWritableExtent* extent
    );

    // Begin writing at some location.
    // No other methods should be called until this is called.
    virtual task<> Write(
        ExtentOffset offset,
        size_t count
    ) override;

    virtual ZeroCopyOutputStream* Stream(
    ) override;

    // Commit the data to be flushed later.  The task will complete 
    // quickly but it's possible that no IO was done.
    virtual task<> Commit(
    ) override;

    // Ensure all the data written by this instance is persisted.
    virtual task<> Flush(
    ) override;

    virtual void ReturnToPool(
    ) override;
};

class MemoryMappedWritableExtent
    :
    public IWritableExtent
{
    friend class MemoryMappedWriteBuffer;
    string m_filename;
    size_t m_blockSize;
    
    async_reader_writer_lock m_lastMappedFullRegionLock;
    size_t m_lastMappedFullRegionSize;
    shared_ptr<mapped_region> m_lastFullRegion;

    struct atomic_flush_region
    {
        shared_ptr<mapped_region> m_mappedRegion;
        std::atomic<ExtentOffset> m_beginning;
        std::atomic<ExtentOffset> m_end;

        atomic_flush_region(
            shared_ptr<mapped_region> mappedRegion,
            ExtentOffset beginning,
            ExtentOffset end
        ) :
            m_mappedRegion(mappedRegion),
            m_beginning(beginning),
            m_end(end)
        {}

        atomic_flush_region(
            atomic_flush_region&& other
        )
            : m_mappedRegion(move(other.m_mappedRegion)),
            m_beginning(other.m_beginning.load(std::memory_order_relaxed)),
            m_end(other.m_end.load(std::memory_order_relaxed))
        {}
    };

    cppcoro::async_mutex m_flushLock;
    cppcoro::shared_task<> m_flushTask;

    // This reader/writer lock should be acquired for read
    // to find and update existing entries in m_flushMap and to read the current m_flushTask.
    // It should be acquired for write
    // to add new entries to the flush map or to replace it or to change m_flushTask.
    async_reader_writer_lock m_flushMapLock;

    typedef std::map<mapped_region*, atomic_flush_region> flush_map_type;
    flush_map_type m_flushMap;
    Schedulers m_schedulers;

    task<> GetWriteRegion(
        ExtentOffset offset,
        size_t count,
        shared_ptr<mapped_region>& region,
        void*& data,
        flush_region& flushRegion);

    void UnsafeAddToFlushMap(
        shared_ptr<mapped_region>& mappedRegion,
        flush_region flushRegion
    );

    [[nodiscard]]
    bool UnsafeUpdateFlushMap(
        shared_ptr<mapped_region>& mappedRegion,
        flush_region flushRegion
    );

    task<> Commit(
        shared_ptr<mapped_region>& mappedRegion,
        flush_region flushRegion
    );

    task<> Flush(
        const flush_map_type& flushMap);

    shared_task<> Flush();

    task<> Flush(
        shared_ptr<mapped_region>& mappedRegion,
        flush_region flushRegion
    );


public:
    MemoryMappedWritableExtent(
        Schedulers schedulers,
        string filename,
        size_t m_blockSize
    );

    virtual task<pooled_ptr<IWriteBuffer>> CreateWriteBuffer(
    ) override;
};

MemoryMappedReadableExtent::MemoryMappedReadableExtent(
    mapped_region mappedRegion
) :
    m_mappedRegion(move(mappedRegion))
{
}

task<pooled_ptr<IReadBuffer>> MemoryMappedReadableExtent::CreateReadBuffer()
{
    co_return pooled_ptr<IReadBuffer>(
        new MemoryMappedReadBuffer(this));
}

MemoryMappedReadBuffer::MemoryMappedReadBuffer(
    MemoryMappedReadableExtent* extent
) :
    m_extent(extent)
{
}

task<> MemoryMappedReadBuffer::Read(
    ExtentOffset offset,
    size_t count
)
{
    if (offset + count > m_extent->m_mappedRegion.get_size())
    {
        throw std::range_error("out of range read");
    }

    char* address = 
        reinterpret_cast<char*>(
            m_extent->m_mappedRegion.get_address())
        + offset;

    int countInt = static_cast<int>(count);
    assert(countInt == count);

    m_inputStream.emplace(
        address,
        countInt);

    co_return;
}

ZeroCopyInputStream* MemoryMappedReadBuffer::Stream()
{
    return &*m_inputStream;
}

void MemoryMappedReadBuffer::ReturnToPool()
{
    delete this;
}

MemoryMappedWriteBuffer::MemoryMappedWriteBuffer(
    MemoryMappedWritableExtent* extent
) :
    m_extent(extent)
{
}

task<> MemoryMappedWriteBuffer::Write(
    ExtentOffset extentOffset,
    size_t size
)
{
    void* data = nullptr;

    co_await m_extent->GetWriteRegion(
        extentOffset,
        size,
        m_mappedRegion,
        data,
        m_flushRegion
    );
    
    int size32 = static_cast<int>(size);
    assert(size32 == size);

    m_stream.emplace(
        data,
        size32);
}

ZeroCopyOutputStream* MemoryMappedWriteBuffer::Stream()
{
    return &*m_stream;
}

task<> MemoryMappedWriteBuffer::Commit()
{
    return m_extent->Commit(
        m_mappedRegion,
        m_flushRegion);
}

task<> MemoryMappedWriteBuffer::Flush()
{
    return m_extent->Flush(
        m_mappedRegion,
        m_flushRegion
    );
}

void MemoryMappedWriteBuffer::ReturnToPool()
{
    delete this;
}

MemoryMappedWritableExtent::MemoryMappedWritableExtent(
    Schedulers schedulers,
    string filename,
    size_t blockSize
) : 
    m_schedulers(schedulers),
    m_filename(filename),
    m_blockSize(blockSize),
    m_lastMappedFullRegionSize(0)
{
    // Start the lazy flush task.
    m_flushTask = Flush();
}

task<pooled_ptr<IWriteBuffer>> MemoryMappedWritableExtent::CreateWriteBuffer()
{
    co_return pooled_ptr<IWriteBuffer>(
        new MemoryMappedWriteBuffer(this));
}

task<> MemoryMappedWritableExtent::GetWriteRegion(
    ExtentOffset offset,
    size_t count,
    shared_ptr<mapped_region>& region,
    void*& data,
    flush_region& flushRegion)
{
    {
        auto mappedRegionLock = m_lastMappedFullRegionLock.reader().scoped_try_lock();

        if (!mappedRegionLock)
        {
            mappedRegionLock = co_await m_lastMappedFullRegionLock.reader().scoped_lock_async();
            co_await m_schedulers.LockScheduler->schedule();
        }

        if ((offset + count < m_lastMappedFullRegionSize))
        {
            region = m_lastFullRegion;
            data = reinterpret_cast<char*>(m_lastFullRegion->get_address()) + offset;
            flushRegion =
            {
                .beginning = offset,
                .end = offset + count,
            };

            co_return;
        }
    }

    {
        auto mappedRegionLock = m_lastMappedFullRegionLock.writer().scoped_try_lock();

        if (!mappedRegionLock)
        {
            mappedRegionLock = co_await m_lastMappedFullRegionLock.writer().scoped_lock_async();
            co_await m_schedulers.LockScheduler->schedule();
        }

        if (offset + count < m_lastMappedFullRegionSize)
        {
            region = m_lastFullRegion;
            data = reinterpret_cast<char*>(m_lastFullRegion->get_address()) + offset;
            flushRegion =
            {
                .beginning = offset,
                .end = offset + count,
            };

            co_return;
        }

        auto newSize = m_lastMappedFullRegionSize;

        newSize = std::max(
            newSize + newSize / 4,
            newSize + m_blockSize);

        newSize = std::max(
            (offset + count),
            newSize);

        newSize = (newSize + m_blockSize - 1) / m_blockSize * m_blockSize;

        {
            std::filebuf filebuf;
            if (!filebuf.open(
                m_filename,
                std::ios::binary | std::ios::app
            ))
            {
                throw std::exception("io error");
            }
        }

        std::filesystem::resize_file(
            m_filename,
            newSize
        );

        file_mapping fileMapping(
            m_filename.c_str(),
            boost::interprocess::read_write
        );

        auto newMappedRegion = make_shared<mapped_region>(
            fileMapping,
            boost::interprocess::read_write);

        m_lastFullRegion = newMappedRegion;
        m_lastMappedFullRegionSize = newSize;

        region = m_lastFullRegion;
        data = reinterpret_cast<char*>(m_lastFullRegion->get_address()) + offset;
        flushRegion =
        {
            .beginning = offset,
            .end = offset + count,
        };

        co_return;
    }
}

void MemoryMappedWritableExtent::UnsafeAddToFlushMap(
    shared_ptr<mapped_region>& mappedRegion,
    flush_region flushRegion
)
{
    auto result = m_flushMap.try_emplace(
        &*mappedRegion,
        atomic_flush_region(
            mappedRegion,
            flushRegion.beginning,
            flushRegion.end)
        );

    if (!result.second)
    {
        bool succeeded = UnsafeUpdateFlushMap(
            mappedRegion,
            flushRegion);

        assert(succeeded);
    }
}

[[nodiscard]]
bool MemoryMappedWritableExtent::UnsafeUpdateFlushMap(
    shared_ptr<mapped_region>& mappedRegion,
    flush_region flushRegion
)
{
    auto existingEntry = m_flushMap.find(
        &*mappedRegion);

    if (existingEntry == m_flushMap.end())
    {
        return false;
    }

    ExtentOffset oldBeginning = existingEntry->second.m_beginning.load(
        std::memory_order_relaxed);

    while (oldBeginning > flushRegion.beginning
        && !existingEntry->second.m_beginning.compare_exchange_weak(
            oldBeginning,
            flushRegion.beginning,
            std::memory_order_relaxed))
    {
    }

    ExtentOffset oldEnd = existingEntry->second.m_end.load(
        std::memory_order_relaxed);

    while (oldEnd < flushRegion.end
        && !existingEntry->second.m_end.compare_exchange_weak(
            oldEnd,
            flushRegion.end,
            std::memory_order_relaxed))
    {
    }

    return true;
}

task<> MemoryMappedWritableExtent::Commit(
    shared_ptr<mapped_region>& mappedRegion,
    flush_region flushRegion
)
{
    {
        auto lock = co_await m_flushMapLock.reader().scoped_lock_async();
        if (UnsafeUpdateFlushMap(
            mappedRegion,
            flushRegion
        ))
        {
            co_return;
        }
    }

    {
        auto lock = co_await m_flushMapLock.writer().scoped_lock_async();
        UnsafeAddToFlushMap(
            mappedRegion,
            flushRegion);
    }
}

task<> MemoryMappedWritableExtent::Flush(
    const flush_map_type& flushMap)
{
    co_await m_schedulers.IoScheduler->schedule();

    for (auto& flushMapEntry : flushMap)
    {
        flushMapEntry.first->flush(
            flushMapEntry.second.m_beginning,
            flushMapEntry.second.m_end - flushMapEntry.second.m_beginning,
            false);
    }

    co_return;
}

shared_task<> MemoryMappedWritableExtent::Flush()
{
    auto flushLock = co_await m_flushLock.scoped_lock_async();

    flush_map_type flushMap;

    {
        auto lock = co_await m_flushMapLock.writer().scoped_lock_async();

        std::swap(
            flushMap,
            m_flushMap
        );

        // Create a new flush task for waiters to await on.
        // We'll flush everything that was queued until now,
        // so new items queued will need to wait on the new flush task.
        m_flushTask = Flush();
    }

    co_await Flush(
        flushMap);
}

task<> MemoryMappedWritableExtent::Flush(
    shared_ptr<mapped_region>& mappedRegion,
    flush_region flushRegion
)
{
    shared_task<> flushTask;
    bool didUpdateFlushMap = false;

    while (!didUpdateFlushMap)
    {
        {
            auto lock = co_await m_flushMapLock.reader().scoped_lock_async();
            if (didUpdateFlushMap = UnsafeUpdateFlushMap(
                mappedRegion,
                flushRegion
            ))
            {
                flushTask = m_flushTask;
            }
        }

        // If we didn't update the flush map, it's because there was no entry 
        // in the map.  It's highly likely that another waiter will be trying
        // to add this flush map entry, so to cut down on the number of
        // times the write lock is acquired we only acquire it if there are
        // no other writers.  If there are other writers waiting or already
        // acquired the lock, just retry on the read path.

        if (!didUpdateFlushMap
            &&
            !m_flushMapLock.writer().has_owner()
            &&
            !m_flushMapLock.writer().has_waiter())
        {
            auto lock = co_await m_flushMapLock.writer().scoped_lock_async();
            UnsafeAddToFlushMap(
                mappedRegion,
                flushRegion);
            didUpdateFlushMap = true;
            flushTask = m_flushTask;
        }
    }

    // This lazily starts the flush task.
    co_await flushTask;
    co_await m_schedulers.ComputeScheduler->schedule();
}

MemoryMappedFileExtentStore::MemoryMappedFileExtentStore(
    Schedulers schedulers,
    std::string extentFilenamePrefix,
    std::string extentFilenameSuffix,
    uint64_t writeBlockSize
) :
    m_schedulers(schedulers),
    m_extentFilenamePrefix(extentFilenamePrefix),
    m_extentFilenameSuffix(extentFilenameSuffix),
    m_writeBlockSize(writeBlockSize)
{
}

std::string MemoryMappedFileExtentStore::GetFilename(
    ExtentName extentName)
{
    std::ostringstream result;

    result
        << m_extentFilenamePrefix;

    if (extentName.has_databaseheaderextentname())
    {
        result
            << std::setw(8)
            << std::setfill('0')
            << extentName.databaseheaderextentname().headercopynumber()
            << ".db";
    }
    else if (extentName.has_indexdataextentname())
    {
        result
            << std::setw(8)
            << std::setfill('0')
            << extentName.indexdataextentname().indexnumber()
            << "_"
            << std::setw(8)
            << extentName.indexdataextentname().partitionnumber()
            << ".dat";
    }
    else if (extentName.has_indexheaderextentname())
    {
        result
            << std::setw(8)
            << std::setfill('0')
            << extentName.indexheaderextentname().indexnumber()
            << "_"
            << std::setw(8)
            << extentName.indexheaderextentname().partitionnumber()
            << ".part";
    }
    else if (extentName.has_logextentname())
    {
        result
            << std::setw(8)
            << std::setfill('0')
            << extentName.logextentname().logextentsequencenumber()
            << ".log";
    }
    else
    {
        assert(false);
    }

    return result.str();
}


task<shared_ptr<IReadableExtent>> MemoryMappedFileExtentStore::OpenExtentForRead(
    std::filesystem::path path)
{
    if (std::filesystem::exists(path))
    {
        file_mapping fileMapping(
            path.string().c_str(),
            boost::interprocess::read_only
        );

        mapped_region mappedRegion(
            fileMapping,
            boost::interprocess::read_only
        );

        co_return make_shared<MemoryMappedReadableExtent>(
            move(mappedRegion)
            );
    }

    co_return make_shared<MemoryMappedReadableExtent>(
        mapped_region());
}

task<shared_ptr<IReadableExtent>> MemoryMappedFileExtentStore::OpenExtentForRead(
    ExtentName extentName)
{
    auto filename = GetFilename(
        extentName);

    return OpenExtentForRead(
        filename);
}

task<shared_ptr<IWritableExtent>> MemoryMappedFileExtentStore::OpenExtentForWrite(
    ExtentName extentName)
{
    co_return make_shared<MemoryMappedWritableExtent>(
        m_schedulers,
        GetFilename(extentName),
        m_writeBlockSize);
}

task<> MemoryMappedFileExtentStore::DeleteExtent(
    ExtentName extentName)
{
    auto filename = GetFilename(
        extentName);

    if (std::filesystem::exists(
        filename
    ))
    {
        std::filesystem::rename(
            filename,
            filename + ".deleted.dat");
    }

    //std::filesystem::remove(
    //    GetFilename(ExtentName));
    co_return;
}

}
